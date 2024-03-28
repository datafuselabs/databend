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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::with_hash_method;
use databend_common_expression::with_mappedhash_method;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashTableConfig;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_sql::executor::physical_plans::AggregateExpand;
use databend_common_sql::executor::physical_plans::AggregateFinal;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::executor::physical_plans::AggregatePartial;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::IndexType;
use databend_common_storage::DataOperator;

use crate::pipelines::processors::transforms::aggregator::build_partition_bucket;
use crate::pipelines::processors::transforms::aggregator::AggregateInjector;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::FinalSingleStateAggregator;
use crate::pipelines::processors::transforms::aggregator::PartialSingleStateAggregator;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillWriter;
use crate::pipelines::processors::transforms::aggregator::TransformExpandGroupingSets;
use crate::pipelines::processors::transforms::aggregator::TransformGroupBySpillWriter;
use crate::pipelines::processors::transforms::aggregator::TransformPartialAggregate;
use crate::pipelines::processors::transforms::aggregator::TransformPartialGroupBy;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_aggregate_expand(&mut self, expand: &AggregateExpand) -> Result<()> {
        self.build_pipeline(&expand.input)?;
        let input_schema = expand.input.output_schema()?;
        let group_bys = expand
            .group_bys
            .iter()
            .take(expand.group_bys.len() - 1) // The last group-by will be virtual column `_grouping_id`
            .map(|i| input_schema.index_of(&i.to_string()))
            .collect::<Result<Vec<_>>>()?;
        let grouping_sets = expand
            .grouping_sets
            .sets
            .iter()
            .map(|sets| {
                sets.iter()
                    .map(|i| {
                        let i = input_schema.index_of(&i.to_string())?;
                        let offset = group_bys.iter().position(|j| *j == i).unwrap();
                        Ok(offset)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut grouping_ids = Vec::with_capacity(grouping_sets.len());
        let mask = (1 << group_bys.len()) - 1;
        for set in grouping_sets {
            let mut id = 0;
            for i in set {
                id |= 1 << i;
            }
            // For element in `group_bys`,
            // if it is in current grouping set: set 0, else: set 1. (1 represents it will be NULL in grouping)
            // Example: GROUP BY GROUPING SETS ((a, b), (a), (b), ())
            // group_bys: [a, b]
            // grouping_sets: [[0, 1], [0], [1], []]
            // grouping_ids: 00, 01, 10, 11
            grouping_ids.push(!id & mask);
        }

        self.main_pipeline.add_transform(|input, output| {
            Ok(TransformExpandGroupingSets::create(
                input,
                output,
                group_bys.clone(),
                grouping_ids.clone(),
            ))
        })
    }

    pub(crate) fn build_aggregate_partial(&mut self, aggregate: &AggregatePartial) -> Result<()> {
        self.build_pipeline(&aggregate.input)?;

        let max_block_size = self.settings.get_max_block_size()?;
        let max_threads = self.settings.get_max_threads()?;

        let enable_experimental_aggregate_hashtable = self
            .settings
            .get_enable_experimental_aggregate_hashtable()?;

        let in_cluster = !self.ctx.get_cluster().is_empty();

        let params = Self::build_aggregator_params(
            aggregate.input.output_schema()?,
            &aggregate.group_by,
            &aggregate.agg_funcs,
            enable_experimental_aggregate_hashtable,
            in_cluster,
            max_block_size as usize,
            None,
        )?;

        if params.group_columns.is_empty() {
            return self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(
                    PartialSingleStateAggregator::try_create(input, output, &params)?,
                ))
            });
        }

        let efficiently_memory = self.settings.get_efficiently_memory_group_by()?;

        let group_cols = &params.group_columns;
        let schema_before_group_by = params.input_schema.clone();
        let sample_block = DataBlock::empty_with_schema(schema_before_group_by);
        let method = DataBlock::choose_hash_method(&sample_block, group_cols, efficiently_memory)?;

        // Need a global atomic to read the max current radix bits hint
        let partial_agg_config = if self.ctx.get_cluster().is_empty() {
            HashTableConfig::default().with_partial(true, max_threads as usize)
        } else {
            HashTableConfig::default()
                .cluster_with_partial(true, self.ctx.get_cluster().nodes.len())
        };

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                match params.aggregate_functions.is_empty() {
                    true => with_mappedhash_method!(|T| match method.clone() {
                        HashMethodKind::T(method) => TransformPartialGroupBy::try_create(
                            self.ctx.clone(),
                            method,
                            input,
                            output,
                            params.clone(),
                            partial_agg_config.clone()
                        ),
                    }),
                    false => with_mappedhash_method!(|T| match method.clone() {
                        HashMethodKind::T(method) => TransformPartialAggregate::try_create(
                            self.ctx.clone(),
                            method,
                            input,
                            output,
                            params.clone(),
                            partial_agg_config.clone()
                        ),
                    }),
                }?,
            ))
        })?;

        // If cluster mode, spill write will be completed in exchange serialize, because we need scatter the block data first
        if self.ctx.get_cluster().is_empty() {
            let operator = DataOperator::instance().operator();
            let location_prefix = query_spill_prefix(self.ctx.get_tenant().name());
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(
                    match params.aggregate_functions.is_empty() {
                        true => with_mappedhash_method!(|T| match method.clone() {
                            HashMethodKind::T(method) => TransformGroupBySpillWriter::create(
                                self.ctx.clone(),
                                input,
                                output,
                                method,
                                operator.clone(),
                                location_prefix.clone()
                            ),
                        }),
                        false => with_mappedhash_method!(|T| match method.clone() {
                            HashMethodKind::T(method) => TransformAggregateSpillWriter::create(
                                self.ctx.clone(),
                                input,
                                output,
                                method,
                                operator.clone(),
                                params.clone(),
                                location_prefix.clone()
                            ),
                        }),
                    },
                ))
            })?;
        }

        self.exchange_injector = match params.aggregate_functions.is_empty() {
            true => with_mappedhash_method!(|T| match method.clone() {
                HashMethodKind::T(method) =>
                    AggregateInjector::<_, ()>::create(self.ctx.clone(), method, params.clone()),
            }),
            false => with_mappedhash_method!(|T| match method.clone() {
                HashMethodKind::T(method) =>
                    AggregateInjector::<_, usize>::create(self.ctx.clone(), method, params.clone()),
            }),
        };

        Ok(())
    }

    pub(crate) fn build_aggregate_final(&mut self, aggregate: &AggregateFinal) -> Result<()> {
        let max_block_size = self.settings.get_max_block_size()?;
        let enable_experimental_aggregate_hashtable = self
            .settings
            .get_enable_experimental_aggregate_hashtable()?;
        let in_cluster = !self.ctx.get_cluster().is_empty();
        let params = Self::build_aggregator_params(
            aggregate.before_group_by_schema.clone(),
            &aggregate.group_by,
            &aggregate.agg_funcs,
            enable_experimental_aggregate_hashtable,
            in_cluster,
            max_block_size as usize,
            aggregate.limit,
        )?;

        if params.group_columns.is_empty() {
            self.build_pipeline(&aggregate.input)?;
            self.main_pipeline.try_resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(
                    FinalSingleStateAggregator::try_create(input, output, &params)?,
                ))
            })?;

            return Ok(());
        }

        let efficiently_memory = self.settings.get_efficiently_memory_group_by()?;

        let group_cols = &params.group_columns;
        let schema_before_group_by = params.input_schema.clone();
        let sample_block = DataBlock::empty_with_schema(schema_before_group_by);
        let method = DataBlock::choose_hash_method(&sample_block, group_cols, efficiently_memory)?;

        let old_inject = self.exchange_injector.clone();

        match params.aggregate_functions.is_empty() {
            true => with_hash_method!(|T| match method {
                HashMethodKind::T(v) => {
                    let input: &PhysicalPlan = &aggregate.input;
                    if matches!(input, PhysicalPlan::ExchangeSource(_)) {
                        self.exchange_injector = AggregateInjector::<_, ()>::create(
                            self.ctx.clone(),
                            v.clone(),
                            params.clone(),
                        );
                    }

                    self.build_pipeline(&aggregate.input)?;
                    self.exchange_injector = old_inject;
                    build_partition_bucket::<_, ()>(v, &mut self.main_pipeline, params.clone())
                }
            }),
            false => with_hash_method!(|T| match method {
                HashMethodKind::T(v) => {
                    let input: &PhysicalPlan = &aggregate.input;
                    if matches!(input, PhysicalPlan::ExchangeSource(_)) {
                        self.exchange_injector = AggregateInjector::<_, usize>::create(
                            self.ctx.clone(),
                            v.clone(),
                            params.clone(),
                        );
                    }
                    self.build_pipeline(&aggregate.input)?;
                    self.exchange_injector = old_inject;
                    build_partition_bucket::<_, usize>(v, &mut self.main_pipeline, params.clone())
                }
            }),
        }
    }

    pub fn build_aggregator_params(
        input_schema: DataSchemaRef,
        group_by: &[IndexType],
        agg_funcs: &[AggregateFunctionDesc],
        enable_experimental_aggregate_hashtable: bool,
        in_cluster: bool,
        max_block_size: usize,
        limit: Option<usize>,
    ) -> Result<Arc<AggregatorParams>> {
        let mut agg_args = Vec::with_capacity(agg_funcs.len());
        let (group_by, group_data_types) = group_by
            .iter()
            .map(|i| {
                let index = input_schema.index_of(&i.to_string())?;
                Ok((index, input_schema.field(index).data_type().clone()))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let aggs: Vec<AggregateFunctionRef> = agg_funcs
            .iter()
            .map(|agg_func| {
                let args = agg_func
                    .arg_indices
                    .iter()
                    .map(|i| {
                        let index = input_schema.index_of(&i.to_string())?;
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>>>()?;
                agg_args.push(args);
                AggregateFunctionFactory::instance().get(
                    agg_func.sig.name.as_str(),
                    agg_func.sig.params.clone(),
                    agg_func.sig.args.clone(),
                )
            })
            .collect::<Result<_>>()?;

        let params = AggregatorParams::try_create(
            input_schema,
            group_data_types,
            &group_by,
            &aggs,
            &agg_args,
            enable_experimental_aggregate_hashtable,
            in_cluster,
            max_block_size,
            limit,
        )?;

        Ok(params)
    }
}
