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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storages_fuse::io::read::InvertedIndexReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::pruning::create_inverted_index_query;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_inverted_index::get_inverted_index_handler;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::RefreshTableIndexInterpreter;
use databend_query::test_kits::append_string_sample_data;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use tantivy::schema::IndexRecordOption;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_inverted_index() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_string_table().await?;

    let number_of_block = 2;
    append_string_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;

    let handler = get_inverted_index_handler();

    let ctx = fixture.new_query_ctx().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let settings = ReadSettings::from_ctx(&table_ctx)?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let table_id = table.get_id();
    let index_name = "idx1".to_string();
    let mut options = BTreeMap::new();
    options.insert("tokenizer".to_string(), "english".to_string());
    options.insert(
        "filters".to_string(),
        "english_stop,english_stemmer,chinese_stop".to_string(),
    );
    let tenant = ctx.get_tenant();

    let req = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant,
        name: index_name.clone(),
        column_ids: vec![0, 1],
        sync_creation: false,
        options: options.clone(),
    };

    let res = handler.do_create_table_index(catalog.clone(), req).await;
    assert!(res.is_ok());

    let refresh_index_plan = RefreshTableIndexPlan {
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name.clone(),
        segment_locs: None,
    };
    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let new_table = table.refresh(ctx.as_ref()).await?;
    let new_fuse_table = FuseTable::do_create(new_table.get_table_info().clone())?;
    let table_schema = new_fuse_table.schema();

    // get index location from new table snapshot
    let new_snapshot = new_fuse_table.read_table_snapshot().await?;
    assert!(new_snapshot.is_some());
    let new_snapshot = new_snapshot.unwrap();

    let table_info = new_table.get_table_info();
    let table_indexes = &table_info.meta.indexes;
    let table_index = table_indexes.get(&index_name);
    assert!(table_index.is_some());
    let table_index = table_index.unwrap();
    let index_version = table_index.version.clone();

    let segment_reader =
        MetaReaders::segment_info_reader(new_fuse_table.get_operator(), table_schema.clone());

    let mut block_metas = vec![];
    for (segment_loc, ver) in &new_snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: segment_loc.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        for block_meta in segment_info.block_metas()? {
            block_metas.push(block_meta);
        }
    }
    assert_eq!(block_metas.len(), 1);
    let block_meta = &block_metas[0];

    let dal = new_fuse_table.get_operator_ref();
    let query_fields = vec![("title".to_string(), None), ("content".to_string(), None)];
    let index_schema = DataSchema::new(vec![
        DataField::new("title", DataType::String),
        DataField::new("content", DataType::String),
    ]);

    let index_loc = TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
        &block_meta.location.0,
        &index_name,
        &index_version,
    );

    let has_score = true;
    let need_position = false;
    let mut field_ids = HashSet::new();
    field_ids.insert(0);
    field_ids.insert(1);
    let index_record = IndexRecordOption::WithFreqsAndPositions;

    let index_reader = InvertedIndexReader::create(dal.clone());

    let queries = vec![
        ("rust".to_string(), vec![0, 1]),
        ("java".to_string(), vec![2]),
        ("data".to_string(), vec![1, 4, 5]),
    ];

    for (query_text, ids) in queries.into_iter() {
        let inverted_index_info = InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: options.clone(),
            index_schema: index_schema.clone(),
            query_fields: query_fields.clone(),
            query_text,
            has_score,
            inverted_index_option: None,
        };

        let (query, fuzziness, tokenizer_manager) =
            create_inverted_index_query(&inverted_index_info)?;

        let matched_rows = index_reader
            .clone()
            .do_filter(
                &settings,
                need_position,
                has_score,
                query.box_clone(),
                &field_ids,
                &index_record,
                &fuzziness,
                tokenizer_manager,
                block_meta.row_count,
                &index_loc,
            )
            .await?;
        assert!(matched_rows.is_some());
        let matched_rows = matched_rows.unwrap();
        assert_eq!(matched_rows.len(), ids.len());
        for (matched_row, id) in matched_rows.iter().zip(ids.iter()) {
            assert_eq!(matched_row.0, *id);
        }
    }

    Ok(())
}
