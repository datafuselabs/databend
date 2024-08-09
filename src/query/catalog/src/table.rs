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

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AbortChecker;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use databend_common_io::constants::DEFAULT_BLOCK_MIN_ROWS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_types::MetaId;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::Histogram;
use databend_common_storage::StorageMetrics;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ChangeType;

use crate::plan::DataSourceInfo;
use crate::plan::DataSourcePlan;
use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::PushDownInfo;
use crate::plan::ReclusterParts;
use crate::plan::StreamColumn;
use crate::statistics::BasicColumnStatistics;
use crate::table_args::TableArgs;
use crate::table_context::TableContext;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn engine(&self) -> &str {
        self.get_table_info().engine()
    }

    /// Whether the table engine supports the given internal column.
    fn supported_internal_column(&self, _column_id: ColumnId) -> bool {
        false
    }

    fn schema(&self) -> Arc<TableSchema> {
        self.get_table_info().schema()
    }

    fn options(&self) -> &BTreeMap<String, String> {
        self.get_table_info().options()
    }

    fn field_comments(&self) -> &Vec<String> {
        self.get_table_info().field_comments()
    }

    fn get_id(&self) -> MetaId {
        self.get_table_info().ident.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any;

    fn get_table_info(&self) -> &TableInfo;

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::TableSource(self.get_table_info().clone())
    }

    /// get_data_metrics will get data metrics from table.
    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        None
    }

    /// whether column prune(projection) can help in table read
    fn support_column_projection(&self) -> bool {
        false
    }

    /// whether table has the exact number of total rows
    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    fn cluster_keys(&self, _ctx: Arc<dyn TableContext>) -> Vec<RemoteExpr<String>> {
        vec![]
    }

    fn change_tracking_enabled(&self) -> bool {
        false
    }

    fn stream_columns(&self) -> Vec<StreamColumn> {
        vec![]
    }

    fn schema_with_stream(&self) -> Arc<TableSchema> {
        let mut fields = self.schema().fields().clone();
        for stream_column in self.stream_columns().iter() {
            fields.push(stream_column.table_field());
        }
        Arc::new(TableSchema {
            fields,
            ..self.schema().as_ref().clone()
        })
    }

    /// Whether the table engine supports prewhere optimization.
    /// only Fuse Engine supports this.
    fn support_prewhere(&self) -> bool {
        false
    }

    fn support_index(&self) -> bool {
        false
    }

    /// Whether the table engine supports virtual columns optimization.
    fn support_virtual_columns(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn alter_table_cluster_keys(
        &self,
        ctx: Arc<dyn TableContext>,
        cluster_key: String,
    ) -> Result<()> {
        let (_, _) = (ctx, cluster_key);

        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Altering table cluster keys is not supported for the '{}' engine.",
            self.engine()
        )))
    }

    #[async_backtrace::framed]
    async fn drop_table_cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        let _ = ctx;

        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Dropping table cluster keys is not supported for the '{}' engine.",
            self.engine()
        )))
    }

    /// Gather partitions to be scanned according to the push_downs
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let (_, _) = (ctx, push_downs);

        Err(ErrorCode::Unimplemented(format!(
            "The 'read_partitions' operation is not implemented for table '{}' using the '{}' engine.",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    /// Assembly the pipeline of reading data from storage, according to the plan
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let (_, _, _, _) = (ctx, plan, pipeline, put_cache);

        Err(ErrorCode::Unimplemented(format!(
            "The 'read_data' operation is not implemented for the table '{}'. Table engine type: '{}'.",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    /// Assembly the pipeline of appending data to storage
    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let (_, _, _) = (ctx, pipeline, append_mode);

        Err(ErrorCode::Unimplemented(format!(
            "The 'append_data' operation is not available for the table '{}'. Current table engine: '{}'.",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        prev_snapshot_id: Option<SnapshotId>,
        _deduplicated_label: Option<String>,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let (_, _, _, _, _, _) = (
            ctx,
            copied_files,
            update_stream_meta,
            pipeline,
            overwrite,
            prev_snapshot_id,
        );

        Ok(())
    }

    #[async_backtrace::framed]
    async fn truncate(&self, ctx: Arc<dyn TableContext>, pipeline: &mut Pipeline) -> Result<()> {
        let (_, _) = (ctx, pipeline);
        Ok(())
    }

    #[async_backtrace::framed]
    async fn purge(
        &self,
        ctx: Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        num_snapshot_limit: Option<usize>,
        keep_last_snapshot: bool,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let (_, _, _, _, _) = (
            ctx,
            instant,
            num_snapshot_limit,
            keep_last_snapshot,
            dry_run,
        );

        Ok(None)
    }

    async fn table_statistics(
        &self,
        ctx: Arc<dyn TableContext>,
        require_fresh: bool,
        change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        let (_, _, _) = (ctx, require_fresh, change_type);

        Ok(None)
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let _ = ctx;

        Ok(Box::new(DummyColumnStatisticsProvider))
    }

    #[async_backtrace::framed]
    async fn navigate_to(
        &self,
        navigation: &TimeNavigation,
        abort_checker: AbortChecker,
    ) -> Result<Arc<dyn Table>> {
        let _ = navigation;
        let _ = abort_checker;

        Err(ErrorCode::Unimplemented(format!(
            "Time travel operation is not supported for the table '{}', which uses the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn generate_changes_query(
        &self,
        ctx: Arc<dyn TableContext>,
        database_name: &str,
        table_name: &str,
        consume: bool,
    ) -> Result<String> {
        let (_, _, _, _) = (ctx, database_name, table_name, consume);

        Err(ErrorCode::Unimplemented(format!(
            "Change tracking operation is not supported for the table '{}', which uses the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn get_block_thresholds(&self) -> BlockThresholds {
        BlockThresholds {
            max_rows_per_block: DEFAULT_BLOCK_MAX_ROWS,
            min_rows_per_block: DEFAULT_BLOCK_MIN_ROWS,
            max_bytes_per_block: DEFAULT_BLOCK_BUFFER_SIZE,
        }
    }

    #[async_backtrace::framed]
    async fn compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<()> {
        let (_, _) = (ctx, limit);

        Err(ErrorCode::Unimplemented(format!(
            "The operation 'compact_segments' is not supported for the table '{}', which is using the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limits: CompactionLimits,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        let (_, _) = (ctx, limits);

        Err(ErrorCode::Unimplemented(format!(
            "The 'compact_blocks' operation is not supported for the table '{}'. Table engine: '{}'.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    // return the selected block num.
    #[async_backtrace::framed]
    async fn recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        let (_, _, _) = (ctx, push_downs, limit);

        Err(ErrorCode::Unimplemented(format!(
            "The 'recluster' operation is not supported for the table '{}'. Table engine: '{}'.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        point: NavigationDescriptor,
    ) -> Result<()> {
        let (_, _) = (ctx, point);

        Err(ErrorCode::Unimplemented(format!(
            "The 'revert_to' operation is not supported for the table '{}'. Table engine: '{}'.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn is_stage_table(&self) -> bool {
        false
    }

    fn result_can_be_cached(&self) -> bool {
        false
    }

    fn broadcast_truncate_to_cluster(&self) -> bool {
        false
    }

    fn is_read_only(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
pub trait TableExt: Table {
    #[async_backtrace::framed]
    async fn refresh(&self, ctx: &dyn TableContext) -> Result<Arc<dyn Table>> {
        let table_info = self.get_table_info();
        let tid = table_info.ident.table_id;
        let catalog = ctx.get_catalog(table_info.catalog()).await?;

        let seqv = catalog.get_table_meta_by_id(tid).await?.ok_or_else(|| {
            let err = UnknownTableId::new(tid, "TableExt::refresh");
            AppError::from(err)
        })?;

        self.refresh_with_seq_meta(ctx, seqv.seq, seqv.data).await
    }

    async fn refresh_with_seq_meta(
        &self,
        ctx: &dyn TableContext,
        seq: u64,
        meta: TableMeta,
    ) -> Result<Arc<dyn Table>> {
        let table_info = self.get_table_info();
        let tid = table_info.ident.table_id;
        let catalog = ctx.get_catalog(table_info.catalog()).await?;

        let table_info = TableInfo {
            ident: TableIdent::new(tid, seq),
            meta,
            ..table_info.clone()
        };
        catalog.get_table_by_info(&table_info)
    }

    fn check_mutable(&self) -> Result<()> {
        if self.is_read_only() {
            let table_info = self.get_table_info();
            Err(ErrorCode::InvalidOperation(format!(
                "Modification not permitted: Table '{}' is READ ONLY, preventing any changes or updates.",
                table_info.name
            )))
        } else {
            Ok(())
        }
    }
}
impl<T: ?Sized> TableExt for T where T: Table {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TimeNavigation {
    TimeTravel(NavigationPoint),
    Changes {
        append_only: bool,
        desc: String,
        at: NavigationPoint,
        end: Option<NavigationPoint>,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NavigationPoint {
    SnapshotID(String),
    TimePoint(DateTime<Utc>),
    StreamInfo(TableInfo),
}

#[derive(Debug, Copy, Clone, Default)]
pub struct TableStatistics {
    pub num_rows: Option<u64>,
    pub data_size: Option<u64>,
    pub data_size_compressed: Option<u64>,
    pub index_size: Option<u64>,
    pub number_of_blocks: Option<u64>,
    pub number_of_segments: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub min: Scalar,
    pub max: Scalar,
    pub null_count: u64,
    pub number_of_distinct_values: u64,
}

pub enum CompactTarget {
    // compact blocks, with optional limit on the number of blocks to be compacted
    Blocks(Option<usize>),
    // compact segments
    Segments,
}

pub enum AppendMode {
    // From INSERT and RECUSTER operation
    Normal,
    // From COPY, Streaming load operation
    Copy,
}

pub trait ColumnStatisticsProvider: Send {
    // returns the statistics of the given column, if any.
    // column_id is just the index of the column in table's schema
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics>;

    // returns the num rows of the table, if any.
    fn num_rows(&self) -> Option<u64>;

    // return histogram if any
    fn histogram(&self, _column_id: ColumnId) -> Option<Histogram> {
        None
    }
}

pub struct DummyColumnStatisticsProvider;

impl ColumnStatisticsProvider for DummyColumnStatisticsProvider {
    fn column_statistics(&self, _column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        None
    }

    fn num_rows(&self) -> Option<u64> {
        None
    }
}

pub struct NavigationDescriptor {
    pub database_name: String,
    pub point: NavigationPoint,
}

use std::collections::HashMap;

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::UnknownTableId;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ParquetTableColumnStatisticsProvider {
    column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
    num_rows: u64,
}

impl ParquetTableColumnStatisticsProvider {
    pub fn new(
        column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
        num_rows: u64,
    ) -> Self {
        Self {
            column_stats,
            num_rows,
        }
    }
}

impl ColumnStatisticsProvider for ParquetTableColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        self.column_stats.get(&column_id).and_then(|s| s.as_ref())
    }

    fn num_rows(&self) -> Option<u64> {
        Some(self.num_rows)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct CompactionLimits {
    pub segment_limit: Option<usize>,
    pub block_limit: Option<usize>,
}

impl CompactionLimits {
    pub fn limits(segment_limit: Option<usize>, block_limit: Option<usize>) -> Self {
        // As n fragmented blocks scattered across at most n segments,
        // when no segment_limit provided, we set it to the same value of block_limit
        let adjusted_segment_limit = segment_limit.or(block_limit);
        CompactionLimits {
            segment_limit: adjusted_segment_limit,
            block_limit,
        }
    }
    pub fn limit_by_num_segments(v: Option<usize>) -> Self {
        CompactionLimits {
            segment_limit: v,
            block_limit: None,
        }
    }

    pub fn limit_by_num_blocks(v: Option<usize>) -> Self {
        let segment_limit = v;
        CompactionLimits {
            segment_limit,
            block_limit: v,
        }
    }
}
