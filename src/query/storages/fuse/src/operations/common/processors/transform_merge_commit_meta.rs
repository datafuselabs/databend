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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;

use crate::operations::AbortOperation;
use crate::operations::CommitMeta;
use crate::operations::ConflictResolveContext;
use crate::operations::SnapshotChanges;
use crate::statistics::merge_statistics;

pub struct TransformMergeCommitMeta {
    to_merged: Vec<CommitMeta>,
    default_cluster_key_id: Option<u32>,
}

impl TransformMergeCommitMeta {
    pub fn create(default_cluster_key_id: Option<u32>) -> Self {
        TransformMergeCommitMeta {
            to_merged: vec![],
            default_cluster_key_id,
        }
    }

    fn merge_conflict_resolve_context(
        l: ConflictResolveContext,
        r: ConflictResolveContext,
        default_cluster_key_id: Option<u32>,
    ) -> ConflictResolveContext {
        match (l, r) {
            (
                ConflictResolveContext::ModifiedSegmentExistsInLatest(l),
                ConflictResolveContext::ModifiedSegmentExistsInLatest(r),
            ) => {
                assert!(!l.check_intersect(&r));

                ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
                    removed_segment_indexes: l
                        .removed_segment_indexes
                        .into_iter()
                        .chain(r.removed_segment_indexes)
                        .collect(),
                    removed_statistics: merge_statistics(
                        &l.removed_statistics,
                        &r.removed_statistics,
                        default_cluster_key_id,
                    ),
                    appended_segments: l
                        .appended_segments
                        .into_iter()
                        .chain(r.appended_segments)
                        .collect(),
                    replaced_segments: l
                        .replaced_segments
                        .into_iter()
                        .chain(r.replaced_segments)
                        .collect(),
                    merged_statistics: merge_statistics(
                        &l.merged_statistics,
                        &r.merged_statistics,
                        default_cluster_key_id,
                    ),
                })
            }
            _ => unreachable!(
                "conflict resolve context to be merged should both be ModifiedSegmentExistsInLatest"
            ),
        }
    }

    pub fn merge_commit_meta(
        l: CommitMeta,
        r: CommitMeta,
        default_cluster_key_id: Option<u32>,
    ) -> CommitMeta {
        assert_eq!(l.table_id, r.table_id, "table id mismatch");
        CommitMeta {
            conflict_resolve_context: Self::merge_conflict_resolve_context(
                l.conflict_resolve_context,
                r.conflict_resolve_context,
                default_cluster_key_id,
            ),
            abort_operation: AbortOperation {
                segments: l
                    .abort_operation
                    .segments
                    .into_iter()
                    .chain(r.abort_operation.segments)
                    .collect(),
                blocks: l
                    .abort_operation
                    .blocks
                    .into_iter()
                    .chain(r.abort_operation.blocks)
                    .collect(),
                bloom_filter_indexes: l
                    .abort_operation
                    .bloom_filter_indexes
                    .into_iter()
                    .chain(r.abort_operation.bloom_filter_indexes)
                    .collect(),
            },
            table_id: l.table_id,
        }
    }
}

impl AccumulatingTransform for TransformMergeCommitMeta {
    const NAME: &'static str = "TransformMergeCommitMeta";

    fn transform(
        &mut self,
        data: databend_common_expression::DataBlock,
    ) -> databend_common_exception::Result<Vec<databend_common_expression::DataBlock>> {
        let commit_meta = CommitMeta::try_from(data)?;
        self.to_merged.push(commit_meta);
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let to_merged = std::mem::take(&mut self.to_merged);
        if to_merged.is_empty() {
            return Ok(vec![]);
        }
        let table_id = to_merged[0].table_id;
        let merged = to_merged
            .into_iter()
            .fold(CommitMeta::empty(table_id), |acc, x| {
                Self::merge_commit_meta(acc, x, self.default_cluster_key_id)
            });
        Ok(vec![merged.into()])
    }
}
