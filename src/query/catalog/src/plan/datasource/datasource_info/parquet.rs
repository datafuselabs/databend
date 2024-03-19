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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet_rs::file::metadata::ParquetMetaData;
use parquet_rs::format::SchemaElement;
use parquet_rs::schema::types;
use parquet_rs::schema::types::SchemaDescPtr;
use parquet_rs::schema::types::SchemaDescriptor;
use parquet_rs::thrift::TSerializable;
use serde::Deserialize;
use thrift::protocol::TCompactInputProtocol;
use thrift::protocol::TCompactOutputProtocol;
use thrift::protocol::TInputProtocol;
use thrift::protocol::TListIdentifier;
use thrift::protocol::TOutputProtocol;
use thrift::protocol::TType;

use crate::plan::datasource::datasource_info::parquet_read_options::ParquetReadOptions;

#[derive(Clone, Debug)]
pub struct FullParquetMeta {
    pub location: String,
    pub size: u64,

    pub meta: Arc<ParquetMetaData>,
    /// Row group level statistics.
    ///
    /// We collect the statistics here to avoid multiple computations of the same parquet meta.
    ///
    /// The container is organized as:
    /// - row_group_level_stats[i][j] is the statistics of the j-th column in the i-th row group of current file.
    pub row_group_level_stats: Option<Vec<HashMap<ColumnId, ColumnStatistics>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ParquetTableInfo {
    pub read_options: ParquetReadOptions,
    pub stage_info: StageInfo,
    pub files_info: StageFilesInfo,

    pub table_info: TableInfo,
    pub arrow_schema: ArrowSchema,
    #[serde(deserialize_with = "deser_schema_desc")]
    #[serde(serialize_with = "ser_schema_desc")]
    pub schema_descr: SchemaDescPtr,
    pub files_to_read: Option<Vec<StageFileInfo>>,
    pub schema_from: String,
    pub compression_ratio: f64,

    // These fields are only used in coordinator node of the cluster,
    // so we don't need to serialize them.
    #[serde(skip)]
    pub leaf_fields: Arc<Vec<TableField>>,
    #[serde(skip)]
    pub parquet_metas: Arc<Mutex<Vec<Arc<FullParquetMeta>>>>,
    #[serde(skip)]
    pub need_stats_provider: bool,
    #[serde(skip)]
    pub max_threads: usize,
    #[serde(skip)]
    pub max_memory_usage: u64,
}

impl ParquetTableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.table_info.schema()
    }

    pub fn desc(&self) -> String {
        self.stage_info.stage_name.clone()
    }
}

fn deser_schema_desc<'de, D>(deserializer: D) -> Result<SchemaDescPtr, D::Error>
where D: serde::Deserializer<'de> {
    let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
    let cursor = Cursor::new(bytes);
    let mut i_prot = TCompactInputProtocol::new(cursor);
    let list_ident = i_prot.read_list_begin().unwrap();
    let mut schema_elements: Vec<SchemaElement> = Vec::with_capacity(list_ident.size as usize);
    for _ in 0..list_ident.size {
        let list_elem = SchemaElement::read_from_in_protocol(&mut i_prot).unwrap();
        schema_elements.push(list_elem);
    }
    i_prot.read_list_end().unwrap();
    let schema = types::from_thrift(&schema_elements).unwrap();
    Ok(Arc::new(SchemaDescriptor::new(schema)))
}

fn ser_schema_desc<S>(schema: &SchemaDescPtr, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    let mut transport = Vec::<u8>::new();
    let mut o_prot = TCompactOutputProtocol::new(&mut transport);
    let schema_elements = types::to_thrift(schema.root_schema()).map_err(|e| {
        serde::ser::Error::custom(format!("Failed to convert schema to thrift: {:?}", e))
    })?;
    o_prot
        .write_list_begin(&TListIdentifier::new(
            TType::Struct,
            schema_elements.len() as i32,
        ))
        .unwrap();
    for e in schema_elements {
        e.write_to_out_protocol(&mut o_prot).unwrap();
    }
    o_prot.write_list_end().unwrap();
    serializer.serialize_bytes(&transport)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Schema as ArrowSchema;
    use databend_common_base::base::tokio::sync::Mutex;
    use databend_common_storage::StageFilesInfo;
    use parquet_rs::basic::ConvertedType;
    use parquet_rs::basic::Repetition;
    use parquet_rs::basic::Type as PhysicalType;
    use parquet_rs::errors::ParquetError;
    use parquet_rs::schema::types::SchemaDescPtr;
    use parquet_rs::schema::types::SchemaDescriptor;
    use parquet_rs::schema::types::Type;

    use super::ParquetTableInfo;

    fn make_desc() -> Result<SchemaDescPtr, ParquetError> {
        let mut fields = vec![];

        let inta = Type::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        fields.push(Arc::new(inta));
        let intb = Type::primitive_type_builder("b", PhysicalType::INT64)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        fields.push(Arc::new(intb));
        let intc = Type::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::UTF8)
            .build()?;
        fields.push(Arc::new(intc));

        // 3-level list encoding
        let item1 = Type::primitive_type_builder("item1", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        let item2 = Type::primitive_type_builder("item2", PhysicalType::BOOLEAN).build()?;
        let item3 = Type::primitive_type_builder("item3", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        let list = Type::group_type_builder("records")
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::LIST)
            .with_fields(vec![Arc::new(item1), Arc::new(item2), Arc::new(item3)])
            .build()?;
        let bag = Type::group_type_builder("bag")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(list)])
            .build()?;
        fields.push(Arc::new(bag));

        let schema = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?;
        Ok(Arc::new(SchemaDescriptor::new(Arc::new(schema))))
    }

    #[test]
    fn test_serde() {
        let schema_descr = make_desc().unwrap();
        let info = ParquetTableInfo {
            schema_descr: schema_descr.clone(),
            read_options: Default::default(),
            stage_info: Default::default(),
            files_info: StageFilesInfo {
                path: "".to_string(),
                files: None,
                pattern: None,
                start_after: None,
            },
            table_info: Default::default(),
            leaf_fields: Arc::new(vec![]),
            arrow_schema: ArrowSchema {
                fields: Default::default(),
                metadata: Default::default(),
            },
            files_to_read: None,
            schema_from: "".to_string(),
            compression_ratio: 0.0,
            parquet_metas: Arc::new(Mutex::new(vec![])),
            need_stats_provider: false,
            max_threads: 1,
            max_memory_usage: 10000,
        };
        let s = serde_json::to_string(&info).unwrap();
        let info = serde_json::from_str::<ParquetTableInfo>(&s).unwrap();
        assert_eq!(info.schema_descr, schema_descr)
    }
}
