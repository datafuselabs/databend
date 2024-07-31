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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::TableScan;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::executor::PhysicalPlanReplacer;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RefreshIndexPlan;
use databend_common_sql::plans::RelOperator;
use databend_common_storages_fuse::operations::AggIndexSink;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseLazyPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::SegmentLocation;
use databend_enterprise_aggregating_index::get_agg_index_handler;
use databend_storages_common_table_meta::meta::Location;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;

pub struct RefreshIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshIndexPlan,
}

impl RefreshIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshIndexPlan) -> Result<Self> {
        Ok(RefreshIndexInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn get_partitions(
        &self,
        plan: &DataSourcePlan,
        fuse_table: Arc<FuseTable>,
    ) -> Result<Option<Partitions>> {
        let snapshot_loc = plan.statistics.snapshot.clone();
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(SegmentLocation {
                    segment_idx: lazy_part_info.segment_index,
                    location: lazy_part_info.segment_location.clone(),
                    snapshot_loc: snapshot_loc.clone(),
                });
            }
        }

        if !lazy_init_segments.is_empty() {
            let table_schema = self.plan.table_info.schema();
            let push_downs = plan.push_downs.clone();
            let ctx = self.ctx.clone();

            let (_statistics, partitions) = fuse_table
                .prune_snapshot_blocks(ctx, push_downs, table_schema, lazy_init_segments, 0)
                .await?;

            return Ok(Some(partitions));
        }

        Ok(None)
    }

    #[async_backtrace::framed]
    async fn get_partitions_with_given_segments(
        &self,
        plan: &DataSourcePlan,
        fuse_table: Arc<FuseTable>,
        segments: Vec<SegmentLocation>,
    ) -> Result<Option<Partitions>> {
        let table_schema = self.plan.table_info.schema();
        let push_downs = plan.push_downs.clone();
        let ctx = self.ctx.clone();

        let (_statistics, partitions) = fuse_table
            .prune_snapshot_blocks(ctx, push_downs, table_schema, segments, 0)
            .await?;

        Ok(Some(partitions))
    }

    #[async_backtrace::framed]
    async fn get_read_source(
        &self,
        query_plan: &PhysicalPlan,
        fuse_table: Arc<FuseTable>,
        segments: Option<Vec<Location>>,
    ) -> Result<Option<DataSourcePlan>> {
        let mut source = vec![];

        let mut collect_read_source = |plan: &PhysicalPlan| {
            if let PhysicalPlan::TableScan(scan) = plan {
                source.push(*scan.source.clone())
            }
        };

        PhysicalPlan::traverse(
            query_plan,
            &mut |_| true,
            &mut collect_read_source,
            &mut |_| {},
        );

        if source.len() != 1 {
            Err(ErrorCode::Internal(
                "Invalid source with multiple table scan when do refresh aggregating index"
                    .to_string(),
            ))
        } else {
            let mut source = source.remove(0);
            let partitions = match segments {
                Some(segment_locs) if !segment_locs.is_empty() => {
                    let segment_locations = create_segment_location_vector(segment_locs, None);
                    self.get_partitions_with_given_segments(&source, fuse_table, segment_locations)
                        .await?
                }
                Some(_) | None => self.get_partitions(&source, fuse_table).await?,
            };
            if let Some(parts) = partitions {
                source.parts = parts;
            }

            // first, sort the partitions by create_on.
            source.parts.partitions.sort_by(|p1, p2| {
                let p1 = FuseBlockPartInfo::from_part(p1).unwrap();
                let p2 = FuseBlockPartInfo::from_part(p2).unwrap();
                p1.create_on.partial_cmp(&p2.create_on).unwrap()
            });

            // then, find the last refresh position.
            let last = source
                .parts
                .partitions
                .binary_search_by(|p| {
                    let fp = FuseBlockPartInfo::from_part(p).unwrap();
                    fp.create_on
                        .partial_cmp(&self.plan.index_meta.updated_on)
                        .unwrap()
                })
                .map_or_else(|i| i, |i| i + 1);

            // finally, skip the refreshed partitions.
            source.parts.partitions = match self.plan.limit {
                Some(limit) => {
                    let end = std::cmp::min(source.parts.len(), last + limit as usize);
                    source.parts.partitions[last..end].to_vec()
                }
                None => source.parts.partitions.into_iter().skip(last).collect(),
            };

            if !source.parts.is_empty() {
                Ok(Some(source))
            } else {
                Ok(None)
            }
        }
    }

    fn update_index_meta(&self, read_source: &DataSourcePlan) -> Result<IndexMeta> {
        let fuse_part = FuseBlockPartInfo::from_part(read_source.parts.partitions.last().unwrap())?;
        let mut index_meta = self.plan.index_meta.clone();
        index_meta.updated_on = fuse_part.create_on;
        Ok(index_meta)
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshIndexInterpreter {
    fn name(&self) -> &str {
        "RefreshIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::AggregateIndex)?;
        let (mut query_plan, output_schema, select_columns) = match self.plan.query_plan.as_ref() {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let schema = if let RelOperator::EvalScalar(eval) = s_expr.plan() {
                    let fields = eval
                        .items
                        .iter()
                        .map(|item| {
                            let ty = item.scalar.data_type()?;
                            Ok(DataField::new(&item.index.to_string(), ty))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    DataSchemaRefExt::create(fields)
                } else {
                    return Err(ErrorCode::SemanticError(
                        "The last operator of the plan of aggregate index query should be EvalScalar",
                    ));
                };

                let mut builder =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                (
                    builder
                        .build(s_expr.as_ref(), bind_context.column_set())
                        .await?,
                    schema,
                    bind_context.columns.clone(),
                )
            }
            _ => {
                return Err(ErrorCode::SemanticError(
                    "Refresh aggregating index encounter Non-Query Plan",
                ));
            }
        };

        let fuse_table = FuseTable::do_create(self.plan.table_info.clone())?;
        let fuse_table: Arc<FuseTable> = fuse_table.into();

        // generate new `DataSourcePlan` that skip refreshed parts.
        let new_read_source = self
            .get_read_source(
                &query_plan,
                fuse_table.clone(),
                self.plan.segment_locs.clone(),
            )
            .await?;

        if new_read_source.is_none() {
            // The partitions are all pruned, we don't need to generate indexes for these partitions (blocks).
            let empty_pipeline = PipelineBuildResult::create();
            return Ok(empty_pipeline);
        }

        let new_read_source = new_read_source.unwrap();

        let new_index_meta = self.update_index_meta(&new_read_source)?;

        let mut replace_read_source = ReadSourceReplacer {
            source: new_read_source,
        };
        query_plan = replace_read_source.replace(&query_plan)?;

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &query_plan).await?;

        let input_schema = query_plan.output_schema()?;

        // Build projection
        let mut projections = Vec::with_capacity(output_schema.num_fields());
        for field in output_schema.fields().iter() {
            let index = input_schema.index_of(field.name())?;
            projections.push(index);
        }
        let num_input_columns = input_schema.num_fields();
        let func_ctx = self.ctx.get_function_context()?;
        build_res.main_pipeline.add_transformer(|| {
            CompoundBlockOperator::new(
                vec![BlockOperator::Project {
                    projection: projections.clone(),
                }],
                func_ctx.clone(),
                num_input_columns,
            )
        });

        // Find the block name column offset in the block.
        let block_name_col = select_columns
            .iter()
            .find(|col| col.column_name.eq_ignore_ascii_case(BLOCK_NAME_COL_NAME))
            .ok_or_else(|| {
                ErrorCode::Internal(
                    "_block_name should be contained in the input of refresh processor",
                )
            })?;
        let block_name_offset = output_schema.index_of(&block_name_col.index.to_string())?;

        let fields = output_schema
            .fields()
            .iter()
            .map(|f| {
                let pos = select_columns
                    .iter()
                    .find(|col| col.index.to_string().eq_ignore_ascii_case(f.name()))
                    .ok_or_else(|| ErrorCode::Internal("should find the corresponding column"))?;
                let field_type = infer_schema_type(f.data_type())?;
                Ok(TableField::new(&pos.column_name, field_type))
            })
            .collect::<Result<Vec<_>>>()?;

        // Build the final sink schema.
        let mut sink_schema = TableSchema::new(fields);
        if !self.plan.user_defined_block_name {
            sink_schema.drop_column(&block_name_col.column_name)?;
        }
        let sink_schema = Arc::new(sink_schema);

        build_res.main_pipeline.try_resize(1)?;
        build_res.main_pipeline.add_sink(|input| {
            AggIndexSink::try_create(
                input,
                fuse_table.get_operator(),
                self.plan.index_id,
                fuse_table.get_write_settings(),
                sink_schema.clone(),
                block_name_offset,
                self.plan.user_defined_block_name,
            )
        })?;

        let ctx = self.ctx.clone();
        let req = UpdateIndexReq {
            index_id: self.plan.index_id,
            index_name: self.plan.index_name.clone(),
            index_meta: new_index_meta,
        };

        build_res
            .main_pipeline
            .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                Ok(_) => GlobalIORuntime::instance()
                    .block_on(async move { modify_last_update(ctx, req).await }),
                Err(error_code) => Err(error_code.clone()),
            });

        return Ok(build_res);
    }
}

async fn modify_last_update(ctx: Arc<QueryContext>, req: UpdateIndexReq) -> Result<()> {
    let catalog = ctx.get_catalog(&ctx.get_current_catalog()).await?;
    let handler = get_agg_index_handler();
    let _ = handler.do_update_index(catalog, req).await?;
    Ok(())
}

struct ReadSourceReplacer {
    source: DataSourcePlan,
}

impl PhysicalPlanReplacer for ReadSourceReplacer {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        let mut plan = plan.clone();
        plan.source = Box::new(self.source.clone());
        Ok(PhysicalPlan::TableScan(plan))
    }
}
