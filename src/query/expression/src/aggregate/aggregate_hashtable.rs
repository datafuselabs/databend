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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;

use super::partitioned_payload::PartitionedPayload;
use super::payload_flush::PayloadFlushState;
use super::probe_state::ProbeState;
use crate::aggregate::payload_row::row_match_columns;
use crate::group_hash_columns;
use crate::new_sel;
use crate::read;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::HashTableConfig;
use crate::Payload;
use crate::StateAddr;
use crate::BATCH_SIZE;
use crate::LOAD_FACTOR;
use crate::MAX_PAGE_SIZE;

const BATCH_ADD_SIZE: usize = 2048;

// The high 16 bits are the salt, the low 48 bits are the pointer address
pub type Entry = u64;

pub struct AggregateHashTable {
    pub payload: PartitionedPayload,
    // use for append rows directly during deserialize
    pub direct_append: bool,
    pub config: HashTableConfig,
    current_radix_bits: u64,
    entries: Vec<Entry>,
    count: usize,
    capacity: usize,
}

unsafe impl Send for AggregateHashTable {}
unsafe impl Sync for AggregateHashTable {}

impl AggregateHashTable {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        config: HashTableConfig,
        arena: Arc<Bump>,
    ) -> Self {
        let capacity = Self::initial_capacity();
        Self::new_with_capacity(group_types, aggrs, config, capacity, arena)
    }

    pub fn new_with_capacity(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        config: HashTableConfig,
        capacity: usize,
        arena: Arc<Bump>,
    ) -> Self {
        Self {
            entries: vec![0u64; capacity],
            count: 0,
            direct_append: false,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new(
                group_types,
                aggrs,
                1 << config.initial_radix_bits,
                vec![arena],
            ),
            capacity,
            config,
        }
    }

    pub fn new_directly(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        config: HashTableConfig,
        capacity: usize,
        arena: Arc<Bump>,
        need_init_entry: bool,
    ) -> Self {
        let entries = if need_init_entry {
            vec![0u64; capacity]
        } else {
            vec![]
        };
        Self {
            entries,
            count: 0,
            direct_append: !need_init_entry,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new(
                group_types,
                aggrs,
                1 << config.initial_radix_bits,
                vec![arena],
            ),
            capacity,
            config,
        }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn add_groups(
        &mut self,
        state: &mut ProbeState,
        group_columns: &[Column],
        params: &[Vec<Column>],
        agg_states: &[Column],
        row_count: usize,
    ) -> Result<usize> {
        if row_count <= BATCH_ADD_SIZE {
            self.add_groups_inner(state, group_columns, params, agg_states, row_count)
        } else {
            let mut new_count = 0;
            for start in (0..row_count).step_by(BATCH_ADD_SIZE) {
                let end = (start + BATCH_ADD_SIZE).min(row_count);
                let step_group_columns = group_columns
                    .iter()
                    .map(|c| c.slice(start..end))
                    .collect::<Vec<_>>();

                let step_params: Vec<Vec<Column>> = params
                    .iter()
                    .map(|c| c.iter().map(|x| x.slice(start..end)).collect())
                    .collect::<Vec<_>>();
                let agg_states = agg_states
                    .iter()
                    .map(|c| c.slice(start..end))
                    .collect::<Vec<_>>();

                new_count += self.add_groups_inner(
                    state,
                    &step_group_columns,
                    &step_params,
                    &agg_states,
                    end - start,
                )?;
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
        agg_states: &[Column],
        row_count: usize,
    ) -> Result<usize> {
        state.row_count = row_count;
        group_hash_columns(group_columns, &mut state.group_hashes);

        let new_group_count = if self.direct_append {
            for idx in 0..row_count {
                state.empty_vector[idx] = idx;
            }
            self.payload.append_rows(state, row_count, group_columns);
            row_count
        } else {
            self.probe_and_create(state, group_columns, row_count)
        };

        if !self.payload.aggrs.is_empty() {
            for i in 0..row_count {
                state.state_places[i] = unsafe {
                    StateAddr::new(read::<u64>(
                        state.addresses[i].add(self.payload.state_offset) as _
                    ) as usize)
                };
            }

            let state_places = &state.state_places.as_slice()[0..row_count];

            if agg_states.is_empty() {
                for ((aggr, params), addr_offset) in self
                    .payload
                    .aggrs
                    .iter()
                    .zip(params.iter())
                    .zip(self.payload.state_addr_offsets.iter())
                {
                    aggr.accumulate_keys(state_places, *addr_offset, params, row_count)?;
                }
            } else {
                for ((aggr, agg_state), addr_offset) in self
                    .payload
                    .aggrs
                    .iter()
                    .zip(agg_states.iter())
                    .zip(self.payload.state_addr_offsets.iter())
                {
                    aggr.batch_merge(state_places, *addr_offset, agg_state)?;
                }
            }
        }

        if self.config.partial_agg && self.capacity >= self.config.max_partial_capacity {
            // check size
            if self.count + BATCH_ADD_SIZE > self.resize_threshold() {
                self.clear_ht();
                self.reset_count();
            }

            // check maybe_repartition
            if self.maybe_repartition() {
                self.clear_ht();
                self.reset_count();
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
        // exceed capacity or should resize
        if row_count + self.count > self.resize_threshold() {
            self.resize(self.capacity * 2);
        }

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        let entries = &mut self.entries;

        let mut group_hashes = new_sel();
        let mut hash_salts = [0_u64; BATCH_SIZE];
        let mask = self.capacity - 1;
        for i in 0..row_count {
            group_hashes[i] = state.group_hashes[i] as usize & mask;
            hash_salts[i] = state.group_hashes[i].get_salt();
            state.no_match_vector[i] = i;
        }

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            // 1. inject new_group_count, new_entry_count, need_compare_count, no_match_count
            for i in 0..remaining_entries {
                let index = state.no_match_vector[i];

                let ht_offset = &mut group_hashes[index];

                let salt = hash_salts[index];

                loop {
                    let entry = &mut entries[*ht_offset];
                    if entry.is_occupied() {
                        if entry.get_salt() == salt {
                            state.group_compare_vector[need_compare_count] = index;
                            need_compare_count += 1;
                            break;
                        } else {
                            *ht_offset += 1;
                            if *ht_offset >= self.capacity {
                                *ht_offset = 0;
                            }
                            continue;
                        }
                    } else {
                        entry.set_salt(salt);
                        state.empty_vector[new_entry_count] = index;
                        new_entry_count += 1;
                        break;
                    }
                }
            }

            // 2. append new_group_count to payload
            if new_entry_count != 0 {
                new_group_count += new_entry_count;

                self.payload
                    .append_rows(state, new_entry_count, group_columns);

                for i in 0..new_entry_count {
                    let index = state.empty_vector[i];
                    let ht_offset = group_hashes[index];
                    let entry = &mut entries[ht_offset];

                    entry.set_pointer(state.addresses[index]);

                    debug_assert_eq!(entry.get_pointer(), state.addresses[index]);
                }
            }

            // 3. set address of compare vector
            if need_compare_count > 0 {
                for i in 0..need_compare_count {
                    let index = state.group_compare_vector[i];
                    let ht_offset = group_hashes[index];
                    let entry = &mut entries[ht_offset];

                    debug_assert!(entry.is_occupied());
                    debug_assert_eq!(entry.get_salt(), hash_salts[index]);
                    state.addresses[index] = entry.get_pointer();
                }

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
            }

            // 5. Linear probing, just increase iter_times
            for i in 0..no_match_count {
                let idx = state.no_match_vector[i];
                let ht_offset = &mut group_hashes[idx];
                *ht_offset += 1;
                if *ht_offset >= self.capacity {
                    *ht_offset = 0;
                }
            }
            remaining_entries = no_match_count;
        }

        self.count += new_group_count;

        new_group_count
    }

    pub fn combine(&mut self, other: Self, flush_state: &mut PayloadFlushState) -> Result<()> {
        self.combine_payloads(&other.payload, flush_state)
    }

    pub fn combine_payloads(
        &mut self,
        payloads: &PartitionedPayload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        for payload in payloads.payloads.iter() {
            self.combine_payload(payload, flush_state)?;
        }
        Ok(())
    }

    pub fn combine_payload(
        &mut self,
        payload: &Payload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        flush_state.clear();

        while payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            let _ = self.probe_and_create(
                &mut flush_state.probe_state,
                &flush_state.group_columns,
                row_count,
            );

            // set state places
            if !self.payload.aggrs.is_empty() {
                for i in 0..row_count {
                    flush_state.probe_state.state_places[i] = unsafe {
                        StateAddr::new(read::<u64>(
                            flush_state.probe_state.addresses[i].add(self.payload.state_offset)
                                as _,
                        ) as usize)
                    };
                }
            }

            let state = &mut flush_state.probe_state;
            let places = &state.state_places.as_slice()[0..row_count];
            let rhses = &flush_state.state_places.as_slice()[0..row_count];
            for (aggr, addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(self.payload.state_addr_offsets.iter())
            {
                aggr.batch_merge_states(places, rhses, *addr_offset)?;
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

    fn maybe_repartition(&mut self) -> bool {
        // already final stage or the max radix bits
        if !self.config.partial_agg || (self.current_radix_bits == self.config.max_radix_bits) {
            return false;
        }

        let bytes_per_partition = self.payload.memory_size() / self.payload.partition_count();

        let mut new_radix_bits = self.current_radix_bits;

        if bytes_per_partition > MAX_PAGE_SIZE * self.config.block_fill_factor as usize {
            new_radix_bits += self.config.repartition_radix_bits_incr;
        }

        loop {
            let current_max_radix_bits = self.config.current_max_radix_bits.load(Ordering::SeqCst);
            if current_max_radix_bits < new_radix_bits
                && self
                    .config
                    .current_max_radix_bits
                    .compare_exchange(
                        current_max_radix_bits,
                        new_radix_bits,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
            {
                continue;
            }
            break;
        }

        let current_max_radix_bits = self.config.current_max_radix_bits.load(Ordering::SeqCst);

        if current_max_radix_bits > self.current_radix_bits {
            let temp_payload = PartitionedPayload::new(
                self.payload.group_types.clone(),
                self.payload.aggrs.clone(),
                1,
                vec![Arc::new(Bump::new())],
            );
            let payload = std::mem::replace(&mut self.payload, temp_payload);
            let mut state = PayloadFlushState::default();

            self.current_radix_bits = current_max_radix_bits;
            self.payload = payload.repartition(1 << current_max_radix_bits, &mut state);
            return true;
        }
        false
    }

    #[inline]
    fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / LOAD_FACTOR) as usize
    }

    // scan payload to reconstruct PointArray
    pub fn resize(&mut self, new_capacity: usize) {
        if self.config.partial_agg {
            if self.capacity == self.config.max_partial_capacity {
                return;
            }
            self.entries = vec![0; new_capacity];
            self.reset_count();
            self.capacity = new_capacity;
            return;
        }

        self.reset_count();
        let mask = (new_capacity - 1) as u64;

        let mut entries = vec![0; new_capacity];

        // iterate over payloads and copy to new entries
        for payload in self.payload.payloads.iter() {
            for page in payload.pages.iter() {
                for idx in 0..page.rows {
                    let row_ptr: *const u8 =
                        unsafe { page.data.as_ptr().add(idx * payload.tuple_size) as _ };

                    let hash: u64 =
                        unsafe { core::ptr::read(row_ptr.add(payload.hash_offset) as _) };

                    let mut hash_slot = hash & mask;
                    while entries[hash_slot as usize].is_occupied() {
                        hash_slot += 1;
                        if hash_slot >= new_capacity as u64 {
                            hash_slot = 0;
                        }
                    }

                    let hs = hash_slot as usize;
                    debug_assert!(!entries[hs].is_occupied());
                    // set value
                    entries[hs].set_salt(hash.get_salt());
                    entries[hs].set_pointer(row_ptr);
                    debug_assert!(entries[hs].is_occupied());
                    debug_assert_eq!(entries[hs].get_pointer(), row_ptr);
                    debug_assert_eq!(entries[hs].get_salt(), hash.get_salt());

                    self.count += 1;
                }
            }
        }

        self.entries = entries;
        self.capacity = new_capacity;
    }

    pub fn initial_capacity() -> usize {
        8192 * 4
    }

    pub fn get_capacity_for_count(count: usize) -> usize {
        ((count.max(Self::initial_capacity()) as f64 * LOAD_FACTOR) as usize).next_power_of_two()
    }

    pub fn clear_ht(&mut self) {
        self.payload.mark_min_cardinality();
        self.entries.fill(0);
    }

    pub fn reset_count(&mut self) {
        self.count = 0;
    }

    pub fn allocated_bytes(&self) -> usize {
        self.payload.memory_size()
            + self
                .payload
                .arenas
                .iter()
                .map(|arena| arena.allocated_bytes())
                .sum::<usize>()
    }
}

/// Upper 16 bits are salt
const SALT_MASK: u64 = 0xFFFF000000000000;
/// Lower 48 bits are the pointer
const POINTER_MASK: u64 = 0x0000FFFFFFFFFFFF;

pub(crate) trait EntryLike {
    fn get_salt(&self) -> u64;
    fn set_salt(&mut self, _salt: u64);
    fn is_occupied(&self) -> bool;

    fn get_pointer(&self) -> *const u8;
    fn set_pointer(&mut self, ptr: *const u8);
}

impl EntryLike for u64 {
    #[inline]
    fn get_salt(&self) -> u64 {
        *self | POINTER_MASK
    }

    #[inline]
    fn set_salt(&mut self, salt: u64) {
        *self = salt;
    }

    #[inline]
    fn is_occupied(&self) -> bool {
        *self != 0
    }

    #[inline]
    fn get_pointer(&self) -> *const u8 {
        (*self & POINTER_MASK) as *const u8
    }

    #[inline]
    fn set_pointer(&mut self, ptr: *const u8) {
        let ptr_value = ptr as u64;
        // Pointer shouldn't use upper bits
        debug_assert!(ptr_value & SALT_MASK == 0);
        // Value should have all 1's in the pointer area
        debug_assert!(*self & POINTER_MASK == POINTER_MASK);

        *self &= ptr_value | SALT_MASK;
    }
}
