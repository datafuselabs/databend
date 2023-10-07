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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::TableSchema;
use common_expression::TopKSorter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::policy::ReadPolicy;
use super::policy::ReadPolicyBuilder;
use super::policy::ReadPolicyImpl;
use super::utils::evaluate_topk;
use super::utils::read_all;
use crate::parquet_rs::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;
use crate::parquet_rs::parquet_reader::topk::BuiltTopK;
use crate::parquet_rs::parquet_reader::topk::ParquetTopK;
use crate::parquet_rs::parquet_reader::utils::bitmap_to_boolean_array;
use crate::parquet_rs::parquet_reader::utils::compute_output_field_paths;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;

pub struct PredicateAndTopkPolicyBuilder {
    predicate: Arc<ParquetPredicate>,
    topk: Option<Arc<ParquetTopK>>,

    // Remain columns
    remain_projection: ProjectionMask,
    remain_field_levels: FieldLevels,
    remain_field_paths: Arc<Option<FieldPaths>>,

    src_schema: DataSchemaRef,
    dst_schema: DataSchemaRef,
}

impl PredicateAndTopkPolicyBuilder {
    pub fn create(
        schema_desc: &SchemaDescriptor,
        predicate: &(Arc<ParquetPredicate>, Vec<usize>),
        topk: Option<&BuiltTopK>,
        output_leaves: &[usize],
        remain_schema: &TableSchema,
        output_schema: &TableSchema,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        let (predicate, predicate_leaves) = predicate;
        let inner_projection = predicate.field_paths().is_some();

        // Compute projections to read columns for each stage (prefetch and remain).
        let mut prefetch_leaves = predicate_leaves.iter().cloned().collect::<HashSet<_>>();
        if let Some(topk) = topk {
            prefetch_leaves.insert(topk.leaf_id);
        }
        // Remove prefetch columns
        // TODO(parquet): reuse inner columns of a nested type.
        // It's a bit complicated now. We just keep all output leaves if it's inner projection.
        let remain_leaves = output_leaves
            .iter()
            .cloned()
            .filter(|i| inner_projection || !prefetch_leaves.contains(i))
            .collect::<Vec<_>>();
        let remain_projection = ProjectionMask::leaves(schema_desc, remain_leaves);

        // Remain fields will not contain predicate fields.
        // We just need to remove the topk column if it is contained in remain fields.
        let remain_fields = if let Some(topk) = topk {
            remain_schema
                .fields()
                .iter()
                .cloned()
                .filter(|f| f.name() != topk.field.name())
                .collect::<Vec<_>>()
        } else {
            remain_schema.fields().clone()
        };

        let (prefetch_fields, topk) = if let Some(topk) = topk {
            let fields = vec![topk.field.clone()]
                .into_iter()
                .chain(predicate.schema().fields().clone().into_iter())
                .collect::<Vec<_>>();
            (fields, Some(topk.topk.clone()))
        } else {
            (predicate.schema().fields().clone(), None)
        };

        let remain_field_levels =
            parquet_to_arrow_field_levels(schema_desc, remain_projection.clone(), None)?;
        let remain_schema = TableSchema::new(remain_fields);
        let remain_field_paths = Arc::new(compute_output_field_paths(
            schema_desc,
            &remain_projection,
            &remain_schema,
            inner_projection,
        )?);

        let mut src_schema = remain_schema;
        src_schema.fields.extend(prefetch_fields);

        let src_schema = Arc::new(DataSchema::from(&src_schema));
        let dst_schema = Arc::new(DataSchema::from(output_schema));

        Ok(Box::new(Self {
            predicate: predicate.clone(),
            topk,
            remain_projection,
            remain_field_levels,
            remain_field_paths,
            src_schema,
            dst_schema,
        }))
    }
}

#[async_trait::async_trait]
impl ReadPolicyBuilder for PredicateAndTopkPolicyBuilder {
    async fn build(
        &self,
        mut row_group: InMemoryRowGroup<'_>,
        mut selection: Option<RowSelection>,
        sorter: &mut Option<TopKSorter>,
        batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        let mut num_rows = selection
            .as_ref()
            .map(|x| x.row_count())
            .unwrap_or(row_group.row_count());
        let mut prefetched = DataBlock::new(vec![], num_rows);

        // Fetch topk columns and check if this row group can be filtered out.
        // Note: we can only check but cannot update the sorter heap because we should evaluate predicate first.
        if let Some((topk, sorter)) = self.topk.as_ref().zip(sorter.as_ref()) {
            row_group
                .fetch(topk.projection(), selection.as_ref())
                .await?;
            // Topk column **must** not be in a nested column.
            let block = read_all(
                &row_group,
                topk.field_levels(),
                selection.clone(),
                &None,
                num_rows,
            )?;
            debug_assert_eq!(block.num_columns(), 1);
            let topk_col = block.columns()[0].value.as_column().unwrap();
            if sorter.never_match_any(topk_col) {
                // All rows in current row group are filtered out.
                return Ok(None);
            }
            prefetched.merge_block(block);
        }

        // Evaluate predicate
        {
            row_group
                .fetch(self.predicate.projection(), selection.as_ref())
                .await?;
            let block = read_all(
                &row_group,
                self.predicate.field_levels(),
                selection.clone(),
                self.predicate.field_paths(),
                num_rows,
            )?;
            let filter = self.predicate.evaluate_block(&block)?;
            if filter.unset_bits() == num_rows {
                // All rows in current row group are filtered out.
                return Ok(None);
            }
            prefetched.merge_block(block);
            prefetched = prefetched.filter_with_bitmap(&filter)?;
            let filter = bitmap_to_boolean_array(filter);
            let sel = RowSelection::from_filters(&[filter]);
            match selection.as_mut() {
                Some(selection) => {
                    *selection = selection.and_then(&sel);
                }
                None => {
                    selection = Some(sel);
                }
            }
            num_rows = prefetched.num_rows();
        }

        // Evaluate topk
        if let Some((topk, sorter)) = self.topk.as_ref().zip(sorter.as_mut()) {
            if let Some(block) = evaluate_topk(prefetched, topk, &mut selection, sorter)? {
                prefetched = block;
            } else {
                // All rows are filtered out.
                return Ok(None);
            }
        }

        // Slice the prefetched block by `batch_size`.
        let mut prefetched_blocks = VecDeque::with_capacity(num_rows.div_ceil(batch_size));
        if num_rows > batch_size {
            for i in (0..num_rows).step_by(batch_size) {
                let end = std::cmp::min(i + batch_size, num_rows);
                let block = prefetched.slice(i..end);
                prefetched_blocks.push_back(block);
            }
        } else {
            prefetched_blocks.push_back(prefetched);
        }

        // Fetch remain columns.
        row_group
            .fetch(&self.remain_projection, selection.as_ref())
            .await?;
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.remain_field_levels,
            &row_group,
            batch_size,
            selection,
        )?;

        Ok(Some(Box::new(PredicateAndTopkPolicy {
            prefetched: prefetched_blocks,
            reader,
            remain_field_paths: self.remain_field_paths.clone(),
            src_schema: self.src_schema.clone(),
            dst_schema: self.dst_schema.clone(),
        })))
    }
}

/// This policy is for the case that predicate is [Some] (topk may be [Some]).
/// We will prefetch predicate and topk columns first
/// and then evaluate predicate and topk to get the final row selection.
/// Finally, we use the final row selection to build the remain data reader.
pub struct PredicateAndTopkPolicy {
    prefetched: VecDeque<DataBlock>,
    reader: ParquetRecordBatchReader,

    /// See the comments of `field_paths` in [`super::NoPrefetchPolicy`].
    remain_field_paths: Arc<Option<FieldPaths>>,
    /// The schema of remain block + prefetched block.
    src_schema: DataSchemaRef,
    /// The final output schema.
    dst_schema: DataSchemaRef,
}

impl ReadPolicy for PredicateAndTopkPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>> {
        let batch = self.reader.next().transpose()?;
        if let Some(batch) = batch {
            debug_assert!(!self.prefetched.is_empty());
            let prefetched = self.prefetched.pop_front().unwrap();
            let mut block = transform_record_batch(&batch, &self.remain_field_paths)?;
            block.merge_block(prefetched);
            let block = block.resort(&self.src_schema, &self.dst_schema)?;
            Ok(Some(block))
        } else {
            debug_assert!(self.prefetched.is_empty());
            Ok(None)
        }
    }
}
