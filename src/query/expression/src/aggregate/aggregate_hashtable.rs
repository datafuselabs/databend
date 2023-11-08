// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A new AggregateHashtable which inspired by duckdb's https://duckdb.org/2022/03/07/aggregate-hashtable.html

use std::sync::Arc;

use common_exception::Result;

use super::payload::Payload;
use super::payload_flush::PayloadFlushState;
use super::probe_state::ProbeState;
use crate::aggregate::payload_row::row_match_columns;
use crate::group_hash_columns;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::StateAddr;

const LOAD_FACTOR: f64 = 1.5;
// hashes layout:
// [SALT][PAGE_NR][PAGE_OFFSET]
// [SALT] are the high bits of the hash value, e.g. 16 for 64 bit hashes
// [PAGE_NR] is the buffer managed payload page index
// [PAGE_OFFSET] is the logical entry offset into said payload page

#[repr(packed)]
#[derive(Default, Debug, Clone, Copy)]
pub struct Entry {
    pub salt: u16,
    pub page_offset: u16,
    pub page_nr: u32,
}

pub struct AggregateHashTable {
    payload: Payload,
    entries: Vec<Entry>,
    capacity: usize,
}

unsafe impl Send for AggregateHashTable {}
unsafe impl Sync for AggregateHashTable {}

impl AggregateHashTable {
    pub fn new(
        arena: Arc<bumpalo::Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
    ) -> Self {
        let capacity = Self::initial_capacity();
        Self {
            entries: Self::new_entries(capacity),
            payload: Payload::new(arena, group_types, aggrs),
            capacity,
        }
    }

    // Faster way to create entries
    // We don't need to extend N zero elements using u64 after we allocate zero spaces
    // due to IsZero Trait(https://stdrs.dev/nightly/x86_64-unknown-linux-gnu/src/alloc/vec/spec_from_elem.rs.html#24)
    fn new_entries(capacity: usize) -> Vec<Entry> {
        let entries = vec![0u64; capacity];
        let (ptr, len, cap) = entries.into_raw_parts();
        unsafe { Vec::from_raw_parts(ptr as *mut Entry, len, cap) }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn add_groups(
        &mut self,
        state: &mut ProbeState,
        group_columns: &[Column],
        params: &[Vec<Column>],
        row_count: usize,
    ) -> Result<usize> {
        const BATCH_ADD_SIZE: usize = 2048;

        if row_count <= BATCH_ADD_SIZE {
            self.add_groups_inner(state, group_columns, params, row_count)
        } else {
            let mut new_count = 0;
            for start in (0..row_count).step_by(BATCH_ADD_SIZE) {
                let end = if start + BATCH_ADD_SIZE > row_count {
                    row_count
                } else {
                    start + BATCH_ADD_SIZE
                };
                let step_group_columns = group_columns
                    .iter()
                    .map(|c| c.slice(start..end))
                    .collect::<Vec<_>>();

                let step_params: Vec<Vec<Column>> = params
                    .iter()
                    .map(|c| c.iter().map(|x| x.slice(start..end)).collect())
                    .collect::<Vec<_>>();

                new_count +=
                    self.add_groups_inner(state, &step_group_columns, &step_params, end - start)?;
            }
            Ok(new_count)
        }
    }

    // Add new groups and combine the states
    fn add_groups_inner(
        &mut self,
        state: &mut ProbeState,
        group_columns: &[Column],
        params: &[Vec<Column>],
        row_count: usize,
    ) -> Result<usize> {
        state.adjust_vector(row_count);
        group_hash_columns(group_columns, &mut state.group_hashes);

        let new_group_count = self.probe_and_create(state, group_columns, row_count);

        if !self.payload.aggrs.is_empty() {
            for i in 0..row_count {
                state.state_places[i] = unsafe {
                    StateAddr::new(core::ptr::read::<u64>(
                        state.addresses[i].add(self.payload.state_offset) as _,
                    ) as usize)
                };
            }

            for ((aggr, params), addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(params.iter())
                .zip(self.payload.state_addr_offsets.iter())
            {
                aggr.accumulate_keys(
                    &state.state_places.as_slice()[0..row_count],
                    *addr_offset,
                    params,
                    row_count,
                )?;
            }
        }

        Ok(new_group_count)
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        group_columns: &[Column],
        row_count: usize,
    ) -> usize {
        if row_count + self.len() > self.capacity
            || row_count + self.len() > self.resize_threshold()
        {
            let mut new_capacity = self.capacity * 2;

            while new_capacity - self.len() <= row_count {
                new_capacity *= 2;
            }
            println!(
                "resize from {} {}  by {}",
                self.capacity, new_capacity, row_count
            );
            self.resize(new_capacity);
        }

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        let mut payload_page_offset = self.len() % self.payload.row_per_page;
        let mut payload_page_nr = (self.len() / self.payload.row_per_page) + 1;

        let mut iter_times = 0;
        let entries = &mut self.entries;
        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            // 1. inject new_group_count, new_entry_count, need_compare_count, no_match_count
            for i in 0..remaining_entries {
                let index = if iter_times == 0 {
                    i
                } else {
                    state.no_match_vector[i]
                };

                let ht_offset =
                    (state.group_hashes[index] as usize + iter_times) & (self.capacity - 1);
                let salt = (state.group_hashes[index] >> (64 - 16)) as u16;

                let entry = &mut entries[ht_offset];

                // cell is empty, could be occupied
                if entry.page_nr == 0 {
                    entry.salt = salt;
                    entry.page_nr = payload_page_nr as u32;
                    entry.page_offset = payload_page_offset as u16;

                    payload_page_offset += 1;

                    if payload_page_offset == self.payload.row_per_page {
                        payload_page_offset = 0;
                        payload_page_nr += 1;

                        self.payload.try_extend_page(payload_page_nr - 1);
                    }

                    state.empty_vector[new_entry_count] = index;
                    new_entry_count += 1;
                } else if entry.salt == salt {
                    let page_ptr = self.payload.get_page_ptr((entry.page_nr - 1) as usize);
                    let page_offset = entry.page_offset as usize * self.payload.tuple_size;
                    state.addresses[index] = unsafe { page_ptr.add(page_offset) };

                    state.group_compare_vector[need_compare_count] = index;
                    need_compare_count += 1;
                } else {
                    state.no_match_vector[no_match_count] = index;
                    no_match_count += 1;
                }
            }

            // 2. append new_group_count to payload
            if new_entry_count != 0 {
                new_group_count += new_entry_count;
                self.payload
                    .append_rows(state, new_entry_count, group_columns);
            }

            // 3. handle need_compare_count
            // already inject addresses to state.addresses

            // 4. compare
            unsafe {
                row_match_columns(
                    group_columns,
                    &state.addresses,
                    &mut state.group_compare_vector,
                    &mut state.temp_vector,
                    need_compare_count,
                    &self.payload.validity_offsets,
                    &self.payload.group_offsets,
                    &mut state.no_match_vector,
                    &mut no_match_count,
                );
            }

            // 5. Linear probing, just increase iter_times
            iter_times += 1;
            remaining_entries = no_match_count;
        }

        // set state places
        if !self.payload.aggrs.is_empty() {
            for i in 0..row_count {
                state.state_places[i] = unsafe {
                    StateAddr::new(core::ptr::read::<u64>(
                        state.addresses[i].add(self.payload.state_offset) as _,
                    ) as usize)
                };
            }
        }

        new_group_count
    }

    pub fn combine(&mut self, other: Self, flush_state: &mut PayloadFlushState) -> Result<()> {
        flush_state.reset();

        while other.payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            let _ = self.probe_and_create(
                &mut flush_state.probe_state,
                &flush_state.group_columns,
                row_count,
            );

            let state = &mut flush_state.probe_state;
            for (aggr, addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(self.payload.state_addr_offsets.iter())
            {
                aggr.batch_merge_states(
                    &state.state_places.as_slice()[0..row_count],
                    &flush_state.state_places.as_slice()[0..row_count],
                    *addr_offset,
                )?;
            }
        }
        Ok(())
    }

    pub fn merge_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        if self.payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            flush_state.aggregate_results.clear();
            for (aggr, addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(self.payload.state_addr_offsets.iter())
            {
                let return_type = aggr.return_type()?;
                let mut builder = ColumnBuilder::with_capacity(&return_type, row_count * 4);

                aggr.batch_merge_result(
                    &flush_state.state_places.as_slice()[0..row_count],
                    *addr_offset,
                    &mut builder,
                )?;
                flush_state.aggregate_results.push(builder.build());
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / LOAD_FACTOR) as usize
    }

    pub fn resize(&mut self, new_capacity: usize) {
        if new_capacity == self.capacity {
            return;
        }

        let mask = (new_capacity - 1) as u64;

        let mut entries = Self::new_entries(new_capacity);
        // iterate over payloads and copy to new entries
        for row in 0..self.len() {
            let row_ptr = self.payload.get_row_ptr(row);
            let hash: u64 = unsafe { core::ptr::read(row_ptr.add(self.payload.hash_offset) as _) };
            let mut hash_slot = hash & mask;

            while entries[hash_slot as usize].page_nr != 0 {
                hash_slot += 1;
                if hash_slot >= self.capacity as u64 {
                    hash_slot = 0;
                }
            }
            let entry = &mut entries[hash_slot as usize];

            entry.page_nr = (row / self.payload.row_per_page) as u32 + 1;
            entry.page_offset = (row % self.payload.row_per_page) as u16;
            entry.salt = (hash >> (64 - 16)) as u16;
        }

        self.entries = entries;
        self.capacity = new_capacity;
    }

    pub fn initial_capacity() -> usize {
        4096
    }

    pub fn get_capacity_for_count(count: usize) -> usize {
        ((count.max(Self::initial_capacity()) as f64 * LOAD_FACTOR) as usize).next_power_of_two()
    }
}
