// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_sql::parse_computed_expr;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::io::write_data;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;

#[async_backtrace::framed]
pub async fn do_refresh_virtual_column(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    virtual_exprs: Vec<String>,
    segment_locs: Option<Vec<Location>>,
) -> Result<()> {
    if virtual_exprs.is_empty() {
        return Ok(());
    }

    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = if let Some(val) = snapshot_opt {
        val
    } else {
        // no snapshot
        return Ok(());
    };

    let table_schema = &fuse_table.get_table_info().meta.schema;

    // Collect source fields used by virtual columns.
    let mut field_indices = Vec::new();
    for (i, f) in table_schema.fields().iter().enumerate() {
        if f.data_type().remove_nullable() != TableDataType::Variant {
            continue;
        }
        let is_src_field = virtual_exprs.iter().any(|v| v.starts_with(f.name()));
        if is_src_field {
            field_indices.push(i);
        }
    }

    if field_indices.is_empty() {
        // no source variant column
        return Ok(());
    }
    let projected_schema = table_schema.project(&field_indices);
    let source_schema = Arc::new(DataSchema::from(&projected_schema));

    let projection = Projection::Columns(field_indices);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    let settings = ReadSettings::from_ctx(&ctx)?;
    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let operator = fuse_table.get_operator_ref();

    // If no segment locations are specified, iterates through all segments
    let segment_locs = if let Some(segment_locs) = segment_locs {
        segment_locs
    } else {
        snapshot.segments.clone()
    };

    // Read source variant columns and extract inner fields as virtual columns.
    for (location, ver) in segment_locs {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver,
                put_cache: false,
            })
            .await?;

        let block_metas = segment_info.block_metas()?;
        for block_meta in block_metas {
            let block = block_reader
                .read_by_meta(&settings, &block_meta, &storage_format)
                .await?;
            let virtual_loc =
                TableMetaLocationGenerator::gen_virtual_block_location(&block_meta.location.0);

            let schema = match storage_format {
                FuseStorageFormat::Parquet => block_reader.sync_read_schema(&virtual_loc),
                FuseStorageFormat::Native => block_reader.sync_read_native_schema(&virtual_loc),
            };

            // if all virtual columns has be generated, we can ignore this block
            let all_generated = if let Some(schema) = schema {
                virtual_exprs
                    .iter()
                    .all(|virtual_name| schema.fields.iter().any(|f| f.name() == virtual_name))
            } else {
                false
            };
            if all_generated {
                continue;
            }

            materialize_virtual_columns(
                ctx.clone(),
                operator,
                &write_settings,
                &virtual_loc,
                source_schema.clone(),
                &virtual_exprs,
                block,
            )
            .await?;
        }
    }

    Ok(())
}

#[async_backtrace::framed]
async fn materialize_virtual_columns(
    ctx: Arc<dyn TableContext>,
    operator: &Operator,
    write_settings: &WriteSettings,
    location: &str,
    source_schema: DataSchemaRef,
    virtual_exprs: &Vec<String>,
    block: DataBlock,
) -> Result<()> {
    let len = block.num_rows();

    let func_ctx = ctx.get_function_context()?;
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let mut virtual_fields = Vec::with_capacity(virtual_exprs.len());
    let mut virtual_columns = Vec::with_capacity(virtual_exprs.len());
    for virtual_expr in virtual_exprs {
        let expr = parse_computed_expr(ctx.clone(), source_schema.clone(), virtual_expr)?;
        let virtual_field = TableField::new(virtual_expr, infer_schema_type(expr.data_type())?);
        virtual_fields.push(virtual_field);

        let value = evaluator.run(&expr)?;
        let virtual_column = BlockEntry::new(expr.data_type().clone(), value);
        virtual_columns.push(virtual_column);
    }
    let virtual_schema = TableSchemaRefExt::create(virtual_fields);
    let virtual_block = DataBlock::new(virtual_columns, len);

    let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
    let _ = serialize_block(write_settings, &virtual_schema, virtual_block, &mut buffer)?;

    write_data(buffer, operator, location).await?;

    Ok(())
}
