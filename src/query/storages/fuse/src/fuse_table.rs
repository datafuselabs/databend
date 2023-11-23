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
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use common_base::base::tokio::sync::RwLock;
use common_catalog::catalog::StorageDescription;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::AppendMode;
use common_catalog::table::ColumnStatisticsProvider;
use common_catalog::table::NavigationDescriptor;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;
use common_sharing::create_share_table_operator;
use common_sql::parse_exprs;
use common_sql::BloomIndexColumns;
use common_storage::init_operator;
use common_storage::DataOperator;
use common_storage::ShareTableConfig;
use common_storage::StorageMetrics;
use common_storage::StorageMetricsLayer;
use log::error;
use log::warn;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::Statistics as FuseStatistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use storages_common_table_meta::meta::TableVersion;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::table_storage_prefix;
use storages_common_table_meta::table::TableCompression;
use storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_READ_ONLY;
use storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;

use crate::fuse_column::FuseTableColumnStatisticsProvider;
use crate::fuse_type::FuseTableType;
use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::table_functions::unwrap_tuple;
use crate::FuseStorageFormat;
use crate::NavigationPoint;
use crate::Table;
use crate::TableStatistics;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::DEFAULT_ROW_PER_PAGE;
use crate::DEFAULT_ROW_PER_PAGE_FOR_BLOCKING;
use crate::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;
use crate::FUSE_OPT_KEY_ROW_PER_PAGE;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT;

#[derive(Clone)]
pub struct FuseTable {
    pub(crate) table_info: TableInfo,
    pub(crate) meta_location_generator: TableMetaLocationGenerator,

    pub(crate) cluster_key_meta: Option<ClusterKey>,
    pub(crate) storage_format: FuseStorageFormat,
    pub(crate) table_compression: TableCompression,
    pub(crate) bloom_index_cols: BloomIndexColumns,

    pub(crate) operator: Operator,
    pub(crate) data_metrics: Arc<StorageMetrics>,

    pub(crate) prev_snapshot: Arc<RwLock<Option<Arc<TableSnapshot>>>>,
    table_type: FuseTableType,
}

impl FuseTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Self::do_create(table_info)?)
    }

    pub fn do_create(table_info: TableInfo) -> Result<Box<FuseTable>> {
        let storage_prefix = Self::parse_storage_prefix(&table_info)?;
        let cluster_key_meta = table_info.meta.cluster_key();

        let (mut operator, table_type) = match table_info.db_type.clone() {
            DatabaseType::ShareDB(share_ident) => {
                let operator = create_share_table_operator(
                    ShareTableConfig::share_endpoint_address(),
                    ShareTableConfig::share_endpoint_token(),
                    &share_ident.tenant,
                    &share_ident.share_name,
                    &table_info.name,
                )?;
                (operator, FuseTableType::SharedReadOnly)
            }
            DatabaseType::NormalDB => {
                let storage_params = table_info.meta.storage_params.clone();
                match storage_params {
                    // External or attached table.
                    Some(sp) => {
                        let table_meta_options = &table_info.meta.options;

                        let table_type = if Self::is_table_attached_read_only(table_meta_options) {
                            FuseTableType::AttachedReadOnly
                        } else if Self::is_table_attached(table_meta_options) {
                            FuseTableType::Attached
                        } else {
                            FuseTableType::External
                        };

                        let operator = init_operator(&sp)?;
                        (operator, table_type)
                    }
                    // Normal table.
                    None => {
                        let operator = DataOperator::instance().operator();
                        (operator, FuseTableType::Standard)
                    }
                }
            }
        };

        let data_metrics = Arc::new(StorageMetrics::default());
        operator = operator.layer(StorageMetricsLayer::new(data_metrics.clone()));

        let storage_format = table_info
            .options()
            .get(OPT_KEY_STORAGE_FORMAT)
            .cloned()
            .unwrap_or_default();

        let table_compression = table_info
            .options()
            .get(OPT_KEY_TABLE_COMPRESSION)
            .cloned()
            .unwrap_or_default();

        let bloom_index_cols = table_info
            .options()
            .get(OPT_KEY_BLOOM_INDEX_COLUMNS)
            .and_then(|s| s.parse::<BloomIndexColumns>().ok())
            .unwrap_or(BloomIndexColumns::All);

        let part_prefix = table_info.meta.part_prefix.clone();

        let meta_location_generator =
            TableMetaLocationGenerator::new(storage_prefix, table_info.ident.seq)
                .with_part_prefix(part_prefix);

        Ok(Box::new(FuseTable {
            table_info,
            meta_location_generator,
            cluster_key_meta,
            bloom_index_cols,
            operator,
            data_metrics,
            storage_format: FuseStorageFormat::from_str(storage_format.as_str())?,
            table_compression: table_compression.as_str().try_into()?,
            table_type,
            prev_snapshot: Arc::new(RwLock::new(None)),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "FUSE".to_string(),
            comment: "FUSE Storage Engine".to_string(),
            support_cluster_key: true,
        }
    }

    pub fn is_native(&self) -> bool {
        matches!(self.storage_format, FuseStorageFormat::Native)
    }

    pub fn meta_location_generator(&self) -> &TableMetaLocationGenerator {
        &self.meta_location_generator
    }

    pub fn get_write_settings(&self) -> WriteSettings {
        let default_rows_per_page = if self.operator.info().native_capability().blocking {
            DEFAULT_ROW_PER_PAGE_FOR_BLOCKING
        } else {
            DEFAULT_ROW_PER_PAGE
        };
        let max_page_size = self.get_option(FUSE_OPT_KEY_ROW_PER_PAGE, default_rows_per_page);
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        WriteSettings {
            storage_format: self.storage_format,
            table_compression: self.table_compression,
            max_page_size,
            block_per_seg,
        }
    }

    pub fn current_table_version(&self) -> TableVersion {
        self.table_info.ident.seq
    }

    /// Get max page size.
    /// For native storage format.
    pub fn get_max_page_size(&self) -> Option<usize> {
        match self.storage_format {
            FuseStorageFormat::Parquet => None,
            FuseStorageFormat::Native => Some(self.get_write_settings().max_page_size),
        }
    }

    pub fn parse_storage_prefix(table_info: &TableInfo) -> Result<String> {
        // if OPT_KE_STORAGE_PREFIX is specified, use it as storage prefix
        if let Some(prefix) = table_info.options().get(OPT_KEY_STORAGE_PREFIX) {
            return Ok(prefix.clone());
        }

        // otherwise, use database id and table id as storage prefix

        let table_id = table_info.ident.table_id;
        let db_id = table_info
            .options()
            .get(OPT_KEY_DATABASE_ID)
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Invalid fuse table, table option {} not found",
                    OPT_KEY_DATABASE_ID
                ))
            })?;
        Ok(table_storage_prefix(db_id, table_id))
    }

    pub fn table_snapshot_statistics_format_version(&self, location: &String) -> u64 {
        TableMetaLocationGenerator::snapshot_version(location)
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn read_table_snapshot_statistics(
        &self,
        snapshot: Option<&Arc<TableSnapshot>>,
    ) -> Result<Option<Arc<TableSnapshotStatistics>>> {
        match snapshot {
            Some(snapshot) => {
                if let Some(loc) = &snapshot.table_statistics_location {
                    let ver = self.table_snapshot_statistics_format_version(loc);
                    let reader = MetaReaders::table_snapshot_statistics_reader(self.get_operator());

                    let load_params = LoadParams {
                        location: loc.clone(),
                        len_hint: None,
                        ver,
                        put_cache: true,
                    };

                    Ok(Some(reader.read(&load_params).await?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    pub async fn read_table_snapshot_by_location(
        &self,
        location: String,
    ) -> Result<Option<Arc<TableSnapshot>>> {
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let ver = self.snapshot_format_version(Some(location.clone())).await?;
        let params = LoadParams {
            location,
            len_hint: None,
            ver,
            put_cache: true,
        };
        Ok(Some(reader.read(&params).await?))
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    pub async fn read_table_snapshot(&self) -> Result<Option<Arc<TableSnapshot>>> {
        if let Some(loc) = self.snapshot_loc().await? {
            self.read_table_snapshot_by_location(loc).await
        } else {
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    pub async fn snapshot_format_version(&self, location_opt: Option<String>) -> Result<u64> {
        let location_opt = if location_opt.is_some() {
            location_opt
        } else {
            self.snapshot_loc().await?
        };
        // If no snapshot location here, indicates that there are no data of this table yet
        // in this case, we just returns the current snapshot version
        Ok(location_opt.map_or(TableSnapshot::VERSION, |loc| {
            TableMetaLocationGenerator::snapshot_version(loc.as_str())
        }))
    }

    #[async_backtrace::framed]
    pub async fn snapshot_loc(&self) -> Result<Option<String>> {
        match self.table_info.db_type {
            DatabaseType::ShareDB(_) => {
                let url = FUSE_TBL_LAST_SNAPSHOT_HINT;
                match self.operator.read(url).await {
                    Ok(data) => {
                        let s = str::from_utf8(&data)?;
                        Ok(Some(s.to_string()))
                    }
                    Err(e) => {
                        error!("read share snapshot location error: {:?}", e);
                        Ok(None)
                    }
                }
            }
            DatabaseType::NormalDB => {
                let options = self.table_info.options();

                if options.get(OPT_KEY_TABLE_ATTACHED_READ_ONLY).is_some() {
                    // if table is read-only attached, parse snapshot location from hint
                    let storage_prefix = options.get(OPT_KEY_STORAGE_PREFIX).unwrap();
                    let hint = format!("{}/{}", storage_prefix, FUSE_TBL_LAST_SNAPSHOT_HINT);
                    let snapshot_loc = {
                        let hint_content = self.operator.read(&hint).await?;
                        let snapshot_full_path = String::from_utf8(hint_content)?;
                        let operator_info = self.operator.info();
                        snapshot_full_path[operator_info.root().len()..].to_string()
                    };
                    Ok(Some(snapshot_loc))
                } else {
                    Ok(options
                        .get(OPT_KEY_SNAPSHOT_LOCATION)
                        // for backward compatibility, we check the legacy table option
                        .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))
                        .cloned())
                }
            }
        }
    }

    pub fn get_operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn get_operator_ref(&self) -> &Operator {
        &self.operator
    }

    pub fn try_from_table(tbl: &dyn Table) -> Result<&FuseTable> {
        tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "expects table of engine FUSE, but got {}",
                tbl.engine()
            ))
        })
    }

    pub fn transient(&self) -> bool {
        self.table_info.meta.options.contains_key("TRANSIENT")
    }

    pub fn cluster_key_str(&self) -> Option<&String> {
        self.cluster_key_meta.as_ref().map(|(_, key)| key)
    }

    pub fn cluster_key_id(&self) -> Option<u32> {
        self.cluster_key_meta.clone().map(|v| v.0)
    }

    pub fn bloom_index_cols(&self) -> BloomIndexColumns {
        self.bloom_index_cols.clone()
    }

    // Check if table is attached.
    fn is_table_attached(table_meta_options: &BTreeMap<String, String>) -> bool {
        table_meta_options
            .get(OPT_KEY_TABLE_ATTACHED_DATA_URI)
            .is_some()
    }

    // Check if table is read-only attached.
    fn is_table_attached_read_only(table_meta_options: &BTreeMap<String, String>) -> bool {
        table_meta_options
            .get(OPT_KEY_TABLE_ATTACHED_READ_ONLY)
            .is_some()
    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn is_local(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        Some(self.data_metrics.clone())
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Vec<RemoteExpr<String>> {
        let table_meta = Arc::new(self.clone());
        if let Some((_, order)) = &self.cluster_key_meta {
            let cluster_keys = parse_exprs(ctx, table_meta.clone(), order).unwrap();
            let cluster_keys = if cluster_keys.len() == 1 {
                unwrap_tuple(&cluster_keys[0]).unwrap_or(cluster_keys)
            } else {
                cluster_keys
            };
            let cluster_keys = cluster_keys
                .iter()
                .map(|k| {
                    k.project_column_ref(|index| {
                        table_meta.schema().field(*index).name().to_string()
                    })
                    .as_remote_expr()
                })
                .collect();
            return cluster_keys;
        }
        vec![]
    }

    #[async_backtrace::framed]
    async fn alter_table_cluster_keys(
        &self,
        ctx: Arc<dyn TableContext>,
        cluster_key_str: String,
    ) -> Result<()> {
        // if new cluster_key_str is the same with old one,
        // no need to change
        if let Some(old_cluster_key_str) = self.cluster_key_str() && *old_cluster_key_str == cluster_key_str{
            return Ok(())
        }
        let mut new_table_meta = self.get_table_info().meta.clone();
        new_table_meta = new_table_meta.push_cluster_key(cluster_key_str);
        let cluster_key_meta = new_table_meta.cluster_key();
        let schema = self.schema().as_ref().clone();

        let prev = self.read_table_snapshot().await?;
        let prev_version = self.snapshot_format_version(None).await?;
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let prev_snapshot_id = prev
            .as_ref()
            .map(|v| (v.snapshot_id, prev_version, v.table_version));
        let prev_statistics_location = prev
            .as_ref()
            .and_then(|v| v.table_statistics_location.clone());
        let (summary, segments) = if let Some(v) = prev {
            (v.summary.clone(), v.segments.clone())
        } else {
            (FuseStatistics::default(), vec![])
        };

        let new_snapshot = TableSnapshot::new(
            &prev_timestamp,
            prev_snapshot_id,
            Some(self.current_table_version()),
            schema,
            summary,
            segments,
            cluster_key_meta,
            prev_statistics_location,
        );

        let mut table_info = self.table_info.clone();
        table_info.meta = new_table_meta;

        self.commit_to_meta_server(ctx.as_ref(), &table_info, new_snapshot, None, &None)
            .await
    }

    #[async_backtrace::framed]
    async fn drop_table_cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        if self.cluster_key_meta.is_none() {
            return Ok(());
        }

        let mut new_table_meta = self.get_table_info().meta.clone();
        new_table_meta.default_cluster_key = None;
        new_table_meta.default_cluster_key_id = None;

        let schema = self.schema().as_ref().clone();

        let prev = self.read_table_snapshot().await?;
        let prev_version = self.snapshot_format_version(None).await?;
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let prev_statistics_location = prev
            .as_ref()
            .and_then(|v| v.table_statistics_location.clone());
        let prev_snapshot_id = prev
            .as_ref()
            .map(|v| (v.snapshot_id, prev_version, v.table_version));
        let (summary, segments) = if let Some(v) = prev {
            (v.summary.clone(), v.segments.clone())
        } else {
            (FuseStatistics::default(), vec![])
        };

        let new_snapshot = TableSnapshot::new(
            &prev_timestamp,
            prev_snapshot_id,
            Some(self.current_table_version()),
            schema,
            summary,
            segments,
            None,
            prev_statistics_location,
        );

        let mut table_info = self.table_info.clone();
        table_info.meta = new_table_meta;

        self.commit_to_meta_server(ctx.as_ref(), &table_info, new_snapshot, None, &None)
            .await
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs, dry_run).await
    }

    #[minitrace::trace]
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline, put_cache)
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
    ) -> Result<()> {
        self.do_append_data(ctx, pipeline, append_mode)
    }

    fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        overwrite: bool,
        prev_snapshot_id: Option<SnapshotId>,
    ) -> Result<()> {
        self.do_commit(ctx, pipeline, copied_files, overwrite, prev_snapshot_id)
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn truncate(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        let purge = false;
        self.do_truncate(ctx, purge).await
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn purge(
        &self,
        ctx: Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        limit: Option<usize>,
        keep_last_snapshot: bool,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        match self.navigate_for_purge(&ctx, instant).await {
            Ok((table, files)) => {
                table
                    .do_purge(&ctx, files, limit, keep_last_snapshot, dry_run)
                    .await
            }
            Err(e) if e.code() == ErrorCode::TABLE_HISTORICAL_DATA_NOT_FOUND => {
                warn!("navigate failed: {:?}", e);
                if dry_run { Ok(Some(vec![])) } else { Ok(None) }
            }
            Err(e) => Err(e),
        }
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn analyze(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        self.do_analyze(&ctx).await
    }

    async fn table_statistics(&self) -> Result<Option<TableStatistics>> {
        let stats = match self.table_type {
            FuseTableType::AttachedReadOnly => {
                let snapshot = self.read_table_snapshot().await?.ok_or_else(|| {
                    // For table created with "ATTACH TABLE ... READ_ONLY"statement, this should be unreachable:
                    // IO or Deserialization related error should have already been thrown, thus
                    // `Internal` error is used.
                    ErrorCode::Internal("Failed to load snapshot of read_only attach table")
                })?;
                let summary = &snapshot.summary;
                TableStatistics {
                    num_rows: Some(summary.row_count),
                    data_size: Some(summary.uncompressed_byte_size),
                    data_size_compressed: Some(summary.compressed_byte_size),
                    index_size: Some(summary.index_size),
                    number_of_blocks: Some(summary.block_count),
                    number_of_segments: Some(snapshot.segments.len() as u64),
                }
            }
            _ => {
                let s = &self.table_info.meta.statistics;
                TableStatistics {
                    num_rows: Some(s.number_of_rows),
                    data_size: Some(s.data_bytes),
                    data_size_compressed: Some(s.compressed_data_bytes),
                    index_size: Some(s.index_data_bytes),
                    number_of_blocks: s.number_of_blocks,
                    number_of_segments: s.number_of_segments,
                }
            }
        };
        Ok(Some(stats))
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(&self) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let provider = if let Some(snapshot) = self.read_table_snapshot().await? {
            let stats = &snapshot.summary.col_stats;
            let table_statistics = self.read_table_snapshot_statistics(Some(&snapshot)).await?;
            if let Some(table_statistics) = table_statistics {
                FuseTableColumnStatisticsProvider::new(
                    stats.clone(),
                    Some(table_statistics.column_distinct_values.clone()),
                    snapshot.summary.row_count,
                )
            } else {
                FuseTableColumnStatisticsProvider::new(
                    stats.clone(),
                    None,
                    snapshot.summary.row_count,
                )
            }
        } else {
            FuseTableColumnStatisticsProvider::default()
        };
        Ok(Box::new(provider))
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn navigate_to(&self, point: &NavigationPoint) -> Result<Arc<dyn Table>> {
        let snapshot_location = if let Some(loc) = self.snapshot_loc().await? {
            loc
        } else {
            // not an error?
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "Empty Table has no historical data",
            ));
        };

        match point {
            NavigationPoint::SnapshotID(snapshot_id) => Ok(self
                .navigate_to_snapshot(snapshot_location, snapshot_id.as_str())
                .await?),
            NavigationPoint::TimePoint(time_point) => Ok(self
                .navigate_to_time_point(snapshot_location, *time_point)
                .await?),
        }
    }

    #[async_backtrace::framed]
    async fn update(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<FieldIndex>,
        update_list: Vec<(FieldIndex, RemoteExpr<String>)>,
        computed_list: BTreeMap<FieldIndex, RemoteExpr<String>>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_update(
            ctx,
            filter,
            col_indices,
            update_list,
            computed_list,
            query_row_id_col,
            pipeline,
        )
        .await
    }

    fn get_block_thresholds(&self) -> BlockThresholds {
        let max_rows_per_block =
            self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_BLOCK_MAX_ROWS);
        let min_rows_per_block = (max_rows_per_block as f64 * 0.8) as usize;
        let max_bytes_per_block = self.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_BUFFER_SIZE,
        );
        BlockThresholds::new(max_rows_per_block, min_rows_per_block, max_bytes_per_block)
    }

    #[async_backtrace::framed]
    async fn compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<()> {
        self.do_compact_segments(ctx, limit).await
    }

    #[async_backtrace::framed]
    async fn compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        self.do_compact_blocks(ctx, limit).await
    }

    #[async_backtrace::framed]
    async fn revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        point: NavigationDescriptor,
    ) -> Result<()> {
        self.do_revert_to(ctx.as_ref(), point).await
    }

    fn support_prewhere(&self) -> bool {
        matches!(self.storage_format, FuseStorageFormat::Native)
    }

    fn support_index(&self) -> bool {
        true
    }

    fn support_virtual_columns(&self) -> bool {
        true
    }

    fn support_row_id_column(&self) -> bool {
        true
    }

    fn result_can_be_cached(&self) -> bool {
        true
    }

    fn is_read_only(&self) -> bool {
        self.table_type.is_readonly()
    }
}
