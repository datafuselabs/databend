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

use std::cell::SyncUnsafeCell;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use databend_common_catalog::plan::compute_row_id_prefix;
use databend_common_catalog::plan::split_prefix;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_hashtable::MergeIntoBlockInfoIndex;
use databend_common_hashtable::RowPtr;
use databend_common_sql::plans::JoinType;
use databend_common_storages_fuse::operations::BlockMetaIndex;
use log::info;

use super::build_state::BuildState;
use super::hash_join_probe_state::MergeIntoChunkPartialUnmodified;
use super::HashJoinBuildState;
use super::HashJoinProbeState;
use super::HashJoinState;
use super::TransformHashJoinProbe;
pub struct MatchedPtr(pub *mut AtomicU8);

unsafe impl Send for MatchedPtr {}
unsafe impl Sync for MatchedPtr {}

pub struct MergeIntoState {
    /// for now we don't support distributed, we will support in the next pr.
    #[allow(unused)]
    pub(crate) merge_into_is_distributed: bool,

    /// FOR MERGE INTO TARGET TABLE AS BUILD SIDE
    /// When merge into target table as build side, we should preserve block info index.
    pub(crate) block_info_index: MergeIntoBlockInfoIndex,
    /// we use matched to tag the matched offset in chunks.
    pub(crate) matched: Vec<u8>,
    /// the matched will be modified concurrently, so we use
    /// atomic_pointers to pointer to matched
    pub(crate) atomic_pointer: MatchedPtr,
    /// chunk_offsets[chunk_idx] stands for the offset of chunk_idx_th chunk in chunks.
    pub(crate) chunk_offsets: Vec<u32>,
}

impl MergeIntoState {
    pub(crate) fn create_merge_into_state(is_distributed: bool) -> SyncUnsafeCell<Self> {
        SyncUnsafeCell::new(MergeIntoState {
            merge_into_is_distributed: is_distributed,
            block_info_index: Default::default(),
            matched: Vec::new(),
            atomic_pointer: MatchedPtr(std::ptr::null_mut()),
            chunk_offsets: Vec::with_capacity(100),
        })
    }
}

impl HashJoinBuildState {
    pub(crate) fn merge_into_try_build_block_info_index(&self, input: DataBlock, old_size: usize) {
        // merge into target table as build side.
        if self
            .hash_join_state
            .merge_into_need_target_partial_modified_scan()
        {
            assert!(input.get_meta().is_some());
            let merge_into_state = unsafe {
                &mut *self
                    .hash_join_state
                    .merge_into_state
                    .as_ref()
                    .unwrap()
                    .get()
            };
            let build_state = unsafe { &*self.hash_join_state.build_state.get() };
            let start_offset = build_state.generation_state.build_num_rows + old_size;
            let end_offset = start_offset + input.num_rows() - 1;
            let block_meta_index =
                BlockMetaIndex::downcast_ref_from(input.get_meta().unwrap()).unwrap();
            let row_prefix = compute_row_id_prefix(
                block_meta_index.segment_idx as u64,
                block_meta_index.block_idx as u64,
            );
            let block_info_index = &mut merge_into_state.block_info_index;
            block_info_index
                .insert_block_offsets((start_offset as u32, end_offset as u32), row_prefix);
        }
    }

    pub(crate) fn merge_into_try_add_chunk_offset(&self, build_state: &mut BuildState) {
        if self
            .hash_join_state
            .merge_into_need_target_partial_modified_scan()
        {
            let merge_into_state = unsafe {
                &mut *self
                    .hash_join_state
                    .merge_into_state
                    .as_ref()
                    .unwrap()
                    .get()
            };
            let chunk_offsets = &mut merge_into_state.chunk_offsets;
            chunk_offsets.push(build_state.generation_state.build_num_rows as u32);
        }
    }

    pub(crate) fn merge_into_try_generate_matched_memory(&self) {
        // generate matched offsets memory.
        if self
            .hash_join_state
            .merge_into_need_target_partial_modified_scan()
        {
            let merge_into_state = unsafe {
                &mut *self
                    .hash_join_state
                    .merge_into_state
                    .as_ref()
                    .unwrap()
                    .get()
            };
            let matched = &mut merge_into_state.matched;
            let build_state = unsafe { &*self.hash_join_state.build_state.get() };
            let atomic_pointer = &mut merge_into_state.atomic_pointer;
            *matched = vec![0; build_state.generation_state.build_num_rows];
            let pointer =
                unsafe { std::mem::transmute::<*mut u8, *mut AtomicU8>(matched.as_mut_ptr()) };
            *atomic_pointer = MatchedPtr(pointer);
        }
    }
}

impl HashJoinProbeState {
    #[inline]
    pub(crate) fn merge_into_check_and_set_matched(
        &self,
        build_indexes: &[RowPtr],
        matched_idx: usize,
        selection: Option<&[u32]>,
    ) -> Result<()> {
        // merge into target table as build side.
        if self
            .hash_join_state
            .merge_into_need_target_partial_modified_scan()
        {
            let merge_into_state = unsafe {
                &*self
                    .hash_join_state
                    .merge_into_state
                    .as_ref()
                    .unwrap()
                    .get()
            };
            let chunk_offsets = &merge_into_state.chunk_offsets;

            let pointer = &merge_into_state.atomic_pointer;
            // add matched indexes.
            if let Some(selection) = selection {
                for idx in selection {
                    let row_ptr = unsafe { build_indexes.get_unchecked(*idx as usize) };
                    let offset = if row_ptr.chunk_index == 0 {
                        row_ptr.row_index as usize
                    } else {
                        chunk_offsets[(row_ptr.chunk_index - 1) as usize] as usize
                            + row_ptr.row_index as usize
                    };

                    let mut old_mactehd_counts =
                        unsafe { (*pointer.0.add(offset)).load(Ordering::Relaxed) };
                    loop {
                        if old_mactehd_counts > 0 {
                            return Err(ErrorCode::UnresolvableConflict(
                                "multi rows from source match one and the same row in the target_table multi times in probe phase",
                            ));
                        }

                        let res = unsafe {
                            (*pointer.0.add(offset)).compare_exchange_weak(
                                old_mactehd_counts,
                                old_mactehd_counts + 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                        };
                        match res {
                            Ok(_) => break,
                            Err(x) => old_mactehd_counts = x,
                        };
                    }
                }
            } else {
                for row_ptr in &build_indexes[0..matched_idx] {
                    let offset = if row_ptr.chunk_index == 0 {
                        row_ptr.row_index as usize
                    } else {
                        chunk_offsets[(row_ptr.chunk_index - 1) as usize] as usize
                            + row_ptr.row_index as usize
                    };

                    let mut old_mactehd_counts =
                        unsafe { (*pointer.0.add(offset)).load(Ordering::Relaxed) };
                    loop {
                        if old_mactehd_counts > 0 {
                            return Err(ErrorCode::UnresolvableConflict(
                                "multi rows from source match one and the same row in the target_table multi times in probe phase",
                            ));
                        }

                        let res = unsafe {
                            (*pointer.0.add(offset)).compare_exchange_weak(
                                old_mactehd_counts,
                                old_mactehd_counts + 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                        };
                        match res {
                            Ok(_) => break,
                            Err(x) => old_mactehd_counts = x,
                        };
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn probe_merge_into_partial_modified_done(&self) -> Result<()> {
        assert!(matches!(
            self.hash_join_state.hash_join_desc.join_type,
            JoinType::Left
        ));
        let old_count = self.wait_probe_counter.fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            // Divide the final scan phase into multiple tasks.
            self.generate_merge_into_final_scan_task()?;
        }
        Ok(())
    }

    pub(crate) fn generate_merge_into_final_scan_task(&self) -> Result<()> {
        let merge_into_state = unsafe {
            &*self
                .hash_join_state
                .merge_into_state
                .as_ref()
                .unwrap()
                .get()
        };
        let block_info_index = &merge_into_state.block_info_index;
        let matched = &merge_into_state.matched;
        let chunks_offsets = &merge_into_state.chunk_offsets;
        let partial_unmodified = block_info_index.gather_all_partial_block_offsets(matched);
        let all_matched_blocks = block_info_index.gather_matched_all_blocks(matched);

        // generate chunks
        info!("chunk len: {}", chunks_offsets.len());
        info!("intervals len: {} ", block_info_index.intervals.len());
        info!(
            "partial unmodified blocks num: {}",
            partial_unmodified.len()
        );
        info!(
            "all_matched_blocks blocks num: {}",
            all_matched_blocks.len()
        );
        let mut tasks = block_info_index.chunk_offsets(&partial_unmodified, chunks_offsets);
        info!("partial unmodified chunk num: {}", tasks.len());
        for prefix in all_matched_blocks {
            // deleted block
            tasks.push((Vec::new(), prefix));
        }
        *self.merge_into_final_partial_unmodified_scan_tasks.write() = tasks.into();
        Ok(())
    }

    pub(crate) fn final_merge_into_partial_unmodified_scan_task(
        &self,
    ) -> Option<MergeIntoChunkPartialUnmodified> {
        let mut tasks = self.merge_into_final_partial_unmodified_scan_tasks.write();
        tasks.pop_front()
    }
}

impl HashJoinState {
    pub(crate) fn merge_into_need_target_partial_modified_scan(&self) -> bool {
        self.merge_into_state.is_some()
    }
}

impl TransformHashJoinProbe {
    pub(crate) fn final_merge_into_partial_unmodified_scan(
        &mut self,
        item: MergeIntoChunkPartialUnmodified,
    ) -> Result<()> {
        // matched whole block, need to delete
        if item.0.is_empty() {
            let prefix = item.1;
            let (segment_idx, block_idx) = split_prefix(prefix);
            info!(
                "matched whole block: segment_idx: {}, block_idx: {}",
                segment_idx, block_idx
            );
            let data_block = DataBlock::empty_with_meta(Box::new(BlockMetaIndex {
                segment_idx: segment_idx as usize,
                block_idx: block_idx as usize,
            }));
            self.output_data_blocks.push_back(data_block);
            return Ok(());
        }
        let merge_into_state = unsafe {
            &*self
                .join_probe_state
                .hash_join_state
                .merge_into_state
                .as_ref()
                .unwrap()
                .get()
        };
        let chunks_offsets = &merge_into_state.chunk_offsets;
        let build_state = unsafe { &*self.join_probe_state.hash_join_state.build_state.get() };
        let chunk_block = &build_state.generation_state.chunks[item.1 as usize];
        let chunk_start = if item.1 == 0 {
            0
        } else {
            chunks_offsets[(item.1 - 1) as usize]
        };
        for (interval, prefix) in item.0 {
            for start in ((interval.0 - chunk_start)..=(interval.1 - chunk_start))
                .step_by(self.max_block_size)
            {
                let end = (interval.1 - chunk_start).min(start + self.max_block_size as u32 - 1);
                let range = (start..=end).collect::<Vec<u32>>();
                let data_block = chunk_block.take(
                    &range,
                    &mut self.probe_state.generation_state.string_items_buf,
                )?;
                assert!(!data_block.is_empty());
                let (segment_idx, block_idx) = split_prefix(prefix);
                info!(
                    "matched partial block: segment_idx: {}, block_idx: {}",
                    segment_idx, block_idx
                );
                let data_block = data_block.add_meta(Some(Box::new(BlockMetaIndex {
                    segment_idx: segment_idx as usize,
                    block_idx: block_idx as usize,
                })))?;
                self.output_data_blocks.push_back(data_block);
            }
        }
        Ok(())
    }
}
