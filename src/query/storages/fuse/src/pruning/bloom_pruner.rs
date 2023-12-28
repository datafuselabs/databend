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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_table_meta::meta::Location;
use log::warn;
use opendal::Operator;

use crate::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns_without_deserialize;
use crate::io::BloomBlockFilterReader;

#[async_trait::async_trait]
pub trait BloomPruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    async fn should_keep(
        &self,
        index_location: &Option<Location>,
        index_length: u64,
        column_ids: Vec<ColumnId>,
    ) -> bool;
}

pub struct BloomPrunerCreator {
    func_ctx: FunctionContext,

    /// indices that should be loaded from filter block
    index_fields: Vec<TableField>,

    /// the expression that would be evaluate
    filter_expression: Expr<String>,

    /// pre calculated digest for constant Scalar
    scalar_map: HashMap<Scalar, u64>,

    /// the data accessor
    dal: Operator,

    /// the schema of data being indexed
    data_schema: TableSchemaRef,
}

impl BloomPrunerCreator {
    pub fn create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        dal: Operator,
        filter_expr: Option<&Expr<String>>,
        bloom_index_cols: BloomIndexColumns,
    ) -> Result<Option<Arc<dyn BloomPruner + Send + Sync>>> {
        if let Some(expr) = filter_expr {
            let bloom_columns_map =
                bloom_index_cols.bloom_index_fields(schema.clone(), BloomIndex::supported_type)?;
            let bloom_column_fields = bloom_columns_map.values().cloned().collect::<Vec<_>>();
            let point_query_cols = BloomIndex::find_eq_columns(expr, bloom_column_fields)?;

            if !point_query_cols.is_empty() {
                // convert to filter column names
                let mut filter_fields = Vec::with_capacity(point_query_cols.len());
                let mut scalar_map = HashMap::<Scalar, u64>::new();
                for (field, scalar, ty) in point_query_cols.into_iter() {
                    filter_fields.push(field);
                    if let Entry::Vacant(e) = scalar_map.entry(scalar.clone()) {
                        let digest = BloomIndex::calculate_scalar_digest(&func_ctx, &scalar, &ty)?;
                        e.insert(digest);
                    }
                }

                let creator = BloomPrunerCreator {
                    func_ctx,
                    index_fields: filter_fields,
                    filter_expression: expr.clone(),
                    scalar_map,
                    dal,
                    data_schema: schema.clone(),
                };
                return Ok(Some(Arc::new(creator)));
            }
        }
        Ok(None)
    }

    // Check a location file is hit or not by bloom filter.
    #[async_backtrace::framed]
    pub async fn apply(
        &self,
        index_location: &Location,
        index_length: u64,
        column_ids_of_indexed_block: Vec<ColumnId>,
    ) -> Result<bool> {
        let version = index_location.1;

        // filter out columns that no longer exist in the indexed block
        let index_columns = self.index_fields.iter().try_fold(
            Vec::with_capacity(self.index_fields.len()),
            |mut acc, field| {
                if column_ids_of_indexed_block.contains(&field.column_id()) {
                    acc.push(BloomIndex::build_filter_column_name(version, field)?);
                }
                Ok::<_, ErrorCode>(acc)
            },
        )?;
        // load the relevant index columns
        let maybe_filter = index_location
            .read_block_filter(self.dal.clone(), &index_columns, index_length)
            .await;

        match maybe_filter {
            Ok(filter) => Ok(BloomIndex::from_filter_block(
                self.func_ctx.clone(),
                filter.filter_schema,
                filter.filters,
                version,
            )?
            .apply(
                self.filter_expression.clone(),
                &self.scalar_map,
                self.data_schema.clone(),
            )? != FilterEvalResult::MustFalse),
            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => {
                // In case that the index is no longer supported, just return true to indicate
                // that the block being pruned should be kept. (Although the caller of this method
                // "FilterPruner::should_keep",  will ignore any exceptions returned)
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    #[async_backtrace::framed]
    pub async fn load_filters_without_deserialize(
        &self,
        index_location: &Location,
        index_length: u64,
        column_ids_of_indexed_block: Vec<ColumnId>,
    ) -> Result<Vec<Vec<u8>>> {
        let version = index_location.1;

        // filter out columns that no longer exist in the indexed block
        let index_columns = self.index_fields.iter().try_fold(
            Vec::with_capacity(self.index_fields.len()),
            |mut acc, field| {
                if column_ids_of_indexed_block.contains(&field.column_id()) {
                    acc.push(BloomIndex::build_filter_column_name(version, field)?);
                }
                Ok::<_, ErrorCode>(acc)
            },
        )?;
        load_bloom_filter_by_columns_without_deserialize(
            self.dal.clone(),
            &index_columns,
            &index_location.0,
            index_length,
        )
        .await
    }
}

#[async_trait::async_trait]
impl BloomPruner for BloomPrunerCreator {
    #[async_backtrace::framed]
    async fn should_keep(
        &self,
        index_location: &Option<Location>,
        index_length: u64,
        column_ids: Vec<ColumnId>,
    ) -> bool {
        if let Some(loc) = index_location {
            // load filter, and try pruning according to filter expression
            match self.apply(loc, index_length, column_ids).await {
                Ok(v) => v,
                Err(e) => {
                    // swallow exceptions intentionally, corrupted index should not prevent execution
                    warn!("failed to apply bloom pruner, returning true. {}", e);
                    true
                }
            }
        } else {
            true
        }
    }
}
