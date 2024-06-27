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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::datavalues::scalar_to_datavalue;
use databend_common_expression::eval_function;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::Versioned;
use parquet::format::FileMetaData;

use crate::filters::BlockBloomFilterIndexVersion;
use crate::filters::BlockFilter;
use crate::filters::Filter;
use crate::filters::FilterBuilder;
use crate::filters::V2BloomBlock;
use crate::filters::Xor8Builder;
use crate::filters::Xor8Filter;
use crate::Index;

#[derive(Clone)]
pub struct BloomIndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
}

impl TryFrom<FileMetaData> for BloomIndexMeta {
    type Error = databend_common_exception::ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let rg = meta.row_groups.remove(0);
        let mut col_metas = Vec::with_capacity(rg.columns.len());
        for x in &rg.columns {
            match &x.meta_data {
                Some(chunk_meta) => {
                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                            dict_page_offset
                        } else {
                            chunk_meta.data_page_offset
                        };
                    let col_len = chunk_meta.total_compressed_size;
                    assert!(
                        col_start >= 0 && col_len >= 0,
                        "column start and length should not be negative"
                    );
                    let num_values = chunk_meta.num_values as u64;
                    let res = SingleColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                    };
                    let column_name = chunk_meta.path_in_schema[0].to_owned();
                    col_metas.push((column_name, res));
                }
                None => {
                    panic!(
                        "expecting chunk meta data while converting ThriftFileMetaData to BloomIndexMeta"
                    )
                }
            }
        }
        col_metas.shrink_to_fit();
        Ok(Self { columns: col_metas })
    }
}

/// BlockFilter represents multiple per-column filters(bloom filter or xor filter etc) for data block.
///
/// By default we create a filter per column for a parquet data file. For columns whose data_type
/// are not applicable for a filter, we skip the creation.
/// That is to say, it is legal to have a BlockFilter with zero columns.
///
/// For example, for the source data block as follows:
/// ```
///         +---name--+--age--+
///         | "Alice" |  20   |
///         | "Bob"   |  30   |
///         +---------+-------+
/// ```
/// We will create table of filters as follows:
/// ```
///         +---Bloom(name)--+--Bloom(age)--+
///         |  123456789abcd |  ac2345bcd   |
///         +----------------+--------------+
/// ```
pub struct BloomIndex {
    pub func_ctx: FunctionContext,

    /// The schema of the filter block.
    ///
    /// It is a sub set of the source table's schema.
    pub filter_schema: TableSchemaRef,

    pub version: u64,

    ///  filters.
    pub filters: Vec<Arc<Xor8Filter>>,

    /// Approximate distinct count of columns generated by xor hash function.
    pub column_distinct_count: HashMap<FieldIndex, usize>,
}

/// FilterExprEvalResult represents the evaluation result of an expression by a filter.
///
/// For example, expression of 'age = 12' should return false is the filter are sure
/// of the nonexistent of value '12' in column 'age'. Otherwise should return 'Uncertain'.
///
/// If the column is not applicable for a filter, like TypeID::struct, Uncertain is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterEvalResult {
    MustFalse,
    Uncertain,
}

impl BloomIndex {
    /// Load a filter directly from the source table's schema and the corresponding filter parquet file.
    #[minitrace::trace]
    pub fn from_filter_block(
        func_ctx: FunctionContext,
        filter_schema: TableSchemaRef,
        filters: Vec<Arc<Xor8Filter>>,
        version: u64,
    ) -> Result<Self> {
        Ok(Self {
            version,
            func_ctx,
            filter_schema,
            filters,
            column_distinct_count: HashMap::new(),
        })
    }

    /// Create a filter block from source data.
    ///
    /// All input blocks should belong to a Parquet file, e.g. the block array represents the parquet file in memory.
    pub fn try_create(
        func_ctx: FunctionContext,
        version: u64,
        data_blocks_tobe_indexed: &[DataBlock],
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ) -> Result<Option<Self>> {
        // TODO refactor :
        // if only current version is allowed, just use the current version
        // instead of passing it in
        assert_eq!(version, BlockFilter::VERSION);

        if data_blocks_tobe_indexed.is_empty() {
            return Err(ErrorCode::BadArguments("block is empty"));
        }

        if data_blocks_tobe_indexed[0].num_columns() == 0 {
            return Ok(None);
        }

        let mut filter_fields = vec![];
        let mut filters = vec![];
        let mut column_distinct_count = HashMap::<usize, usize>::new();
        for (index, field) in bloom_columns_map.into_iter() {
            let field_type = &data_blocks_tobe_indexed[0].get_by_offset(index).data_type;
            if !Xor8Filter::supported_type(field_type) {
                continue;
            }

            let (column, data_type) = match field_type.remove_nullable() {
                DataType::Map(box inner_ty) => {
                    // Add bloom filter for the value of map type
                    let source_columns_iter = data_blocks_tobe_indexed.iter().map(|block| {
                        let value = &block.get_by_offset(index).value;
                        let column = value.convert_to_full_column(field_type, block.num_rows());
                        let map_column = if field_type.is_nullable() {
                            let nullable_column =
                                NullableType::<MapType<AnyType, AnyType>>::try_downcast_column(
                                    &column,
                                )
                                .unwrap();
                            nullable_column.column
                        } else {
                            MapType::<AnyType, AnyType>::try_downcast_column(&column).unwrap()
                        };
                        map_column.values.values
                    });
                    let column = Column::concat_columns(source_columns_iter)?;

                    let val_type = match inner_ty {
                        DataType::Tuple(kv_tys) => kv_tys[1].clone(),
                        _ => unreachable!(),
                    };
                    // Extract JSON value of string type to create bloom index,
                    // other types of JSON value will be ignored.
                    if val_type.remove_nullable() == DataType::Variant {
                        let mut builder = ColumnBuilder::with_capacity(
                            &DataType::Nullable(Box::new(DataType::String)),
                            column.len(),
                        );
                        for val in column.iter() {
                            if let ScalarRef::Variant(v) = val {
                                if let Ok(str_val) = jsonb::to_str(v) {
                                    builder.push(ScalarRef::String(str_val.as_str()));
                                    continue;
                                }
                            }
                            builder.push_default();
                        }
                        let str_column = builder.build();
                        if Self::check_large_string(&str_column) {
                            continue;
                        }
                        let str_type = DataType::Nullable(Box::new(DataType::String));
                        (str_column, str_type)
                    } else {
                        if Self::check_large_string(&column) {
                            continue;
                        }
                        (column, val_type)
                    }
                }
                _ => {
                    let source_columns_iter = data_blocks_tobe_indexed.iter().map(|block| {
                        let value = &block.get_by_offset(index).value;
                        value.convert_to_full_column(field_type, block.num_rows())
                    });
                    let column = Column::concat_columns(source_columns_iter)?;

                    if Self::check_large_string(&column) {
                        continue;
                    }

                    (column, field_type.clone())
                }
            };

            let (column, validity) =
                Self::calculate_nullable_column_digest(&func_ctx, &column, &data_type)?;

            // create filter per column
            let mut filter_builder = Xor8Builder::create();
            if validity.as_ref().map(|v| v.unset_bits()).unwrap_or(0) > 0 {
                let validity = validity.unwrap();
                let it = column.deref().iter().zip(validity.iter()).map(
                    |(v, b)| {
                        if !b { &0 } else { v }
                    },
                );
                filter_builder.add_digests(it);
            } else {
                filter_builder.add_digests(column.deref());
            }
            let filter = filter_builder.build()?;

            if let Some(len) = filter.len() {
                match field.data_type() {
                    TableDataType::Map(_) => {}
                    _ => {
                        column_distinct_count.insert(index, len);
                    }
                }
            }

            let filter_name = Self::build_filter_column_name(version, &field)?;
            filter_fields.push(TableField::new(&filter_name, TableDataType::Binary));
            filters.push(Arc::new(filter));
        }

        if filter_fields.is_empty() {
            return Ok(None);
        }

        let filter_schema = Arc::new(TableSchema::new(filter_fields));

        Ok(Some(Self {
            func_ctx,
            version,
            filter_schema,
            filters,
            column_distinct_count,
        }))
    }

    pub fn serialize_to_data_block(&self) -> Result<DataBlock> {
        let fields = self.filter_schema.fields();
        let mut filter_columns = Vec::with_capacity(fields.len());
        for filter in &self.filters {
            let serialized_bytes = filter.to_bytes()?;
            let filter_value = Value::Scalar(Scalar::Binary(serialized_bytes));
            filter_columns.push(BlockEntry::new(DataType::Binary, filter_value));
        }
        Ok(DataBlock::new(filter_columns, 1))
    }

    /// Apply the predicate expression, return the result.
    /// If we are sure of skipping the scan, return false, e.g. the expression must be false.
    /// This happens when the data doesn't show up in the filter.
    ///
    /// Otherwise return `Uncertain`.
    #[minitrace::trace]
    pub fn apply(
        &self,
        mut expr: Expr<String>,
        scalar_map: &HashMap<Scalar, u64>,
        column_stats: &StatisticsOfColumns,
        data_schema: TableSchemaRef,
    ) -> Result<FilterEvalResult> {
        let mut new_col_id = 1;
        let mut domains = ConstantFolder::full_input_domains(&expr);

        visit_expr_column_eq_constant(
            &mut expr,
            &mut |span, col_name, scalar, ty, return_type| {
                let filter_column = &Self::build_filter_column_name(
                    self.version,
                    data_schema.field_with_name(col_name)?,
                )?;

                // If the column doesn't contain the constant,
                // we rewrite the expression to a new column with `false` domain.
                if self.find(filter_column, scalar, ty, scalar_map)? == FilterEvalResult::MustFalse
                {
                    let new_col_name = format!("__bloom_column_{}_{}", col_name, new_col_id);
                    new_col_id += 1;

                    let bool_domain = Domain::Boolean(BooleanDomain {
                        has_false: true,
                        has_true: false,
                    });
                    let new_domain = if return_type.is_nullable() {
                        // generate `has_null` based on the `null_count` in column statistics.
                        let has_null = match data_schema.column_id_of(col_name) {
                            Ok(col_id) => match column_stats.get(&col_id) {
                                Some(stat) => stat.null_count > 0,
                                None => true,
                            },
                            Err(_) => true,
                        };
                        Domain::Nullable(NullableDomain {
                            has_null,
                            value: Some(Box::new(bool_domain)),
                        })
                    } else {
                        bool_domain
                    };
                    domains.insert(new_col_name.clone(), new_domain);

                    Ok(Some(Expr::ColumnRef {
                        span,
                        id: new_col_name.clone(),
                        data_type: return_type.clone(),
                        display_name: new_col_name,
                    }))
                } else {
                    Ok(None)
                }
            },
        )?;

        let (new_expr, _) =
            ConstantFolder::fold_with_domain(&expr, &domains, &self.func_ctx, &BUILTIN_FUNCTIONS);

        match new_expr {
            Expr::Constant {
                scalar: Scalar::Boolean(false),
                ..
            } => Ok(FilterEvalResult::MustFalse),
            _ => Ok(FilterEvalResult::Uncertain),
        }
    }

    /// calculate digest for column
    pub fn calculate_column_digest(
        func_ctx: &FunctionContext,
        column: &Column,
        data_type: &DataType,
        target_type: &DataType,
    ) -> Result<Column> {
        let (value, _) = eval_function(
            None,
            "siphash",
            [(Value::Column(column.clone()), data_type.clone())],
            func_ctx,
            column.len(),
            &BUILTIN_FUNCTIONS,
        )?;
        let column = value.convert_to_full_column(target_type, column.len());
        Ok(column)
    }

    /// calculate digest for column that may have null values
    ///
    /// returns (column, validity) where column is the digest of the column
    /// and validity is the optional bitmap of the null values
    pub fn calculate_nullable_column_digest(
        func_ctx: &FunctionContext,
        column: &Column,
        data_type: &DataType,
    ) -> Result<(Buffer<u64>, Option<Bitmap>)> {
        Ok(if data_type.is_nullable() {
            let col = Self::calculate_column_digest(
                func_ctx,
                column,
                data_type,
                &DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
            )?;
            let nullable_column = NullableType::<UInt64Type>::try_downcast_column(&col).unwrap();
            (nullable_column.column, Some(nullable_column.validity))
        } else {
            let col = Self::calculate_column_digest(
                func_ctx,
                column,
                data_type,
                &DataType::Number(NumberDataType::UInt64),
            )?;
            let column = UInt64Type::try_downcast_column(&col).unwrap();
            (column, None)
        })
    }

    /// calculate digest for constant scalar
    pub fn calculate_scalar_digest(
        func_ctx: &FunctionContext,
        scalar: &Scalar,
        data_type: &DataType,
    ) -> Result<u64> {
        let (value, _) = eval_function(
            None,
            "siphash",
            [(Value::Scalar(scalar.clone()), data_type.clone())],
            func_ctx,
            1,
            &BUILTIN_FUNCTIONS,
        )?;
        let number_scalar = value.into_scalar().unwrap().into_number().unwrap();
        let digest = u64::try_downcast_scalar(&number_scalar).unwrap();
        Ok(digest)
    }

    /// Find all columns that match the pattern of `col = <constant>` in the expression.
    pub fn find_eq_columns(
        expr: &Expr<String>,
        fields: Vec<TableField>,
    ) -> Result<Vec<(TableField, Scalar, DataType)>> {
        let mut cols = Vec::new();
        visit_expr_column_eq_constant(&mut expr.clone(), &mut |_, col_name, scalar, ty, _| {
            if let Some(v) = fields.iter().find(|f: &&TableField| f.name() == col_name) {
                if Xor8Filter::supported_type(ty) && !scalar.is_null() {
                    cols.push((v.clone(), scalar.clone(), ty.clone()));
                }
            }
            Ok(None)
        })?;
        Ok(cols)
    }

    /// For every applicable column, we will create a filter.
    /// The filter will be stored with field name 'Bloom(column_name)'
    pub fn build_filter_column_name(version: u64, field: &TableField) -> Result<String> {
        let index_version = BlockBloomFilterIndexVersion::try_from(version)?;
        match index_version {
            BlockBloomFilterIndexVersion::V0(_) => Err(ErrorCode::DeprecatedIndexFormat(
                "bloom filter index version(v0) is deprecated",
            )),
            BlockBloomFilterIndexVersion::V2(_) | BlockBloomFilterIndexVersion::V3(_) => {
                Ok(format!("Bloom({})", field.name()))
            }
            BlockBloomFilterIndexVersion::V4(_) => Ok(format!("Bloom({})", field.column_id())),
        }
    }

    fn find(
        &self,
        filter_column: &str,
        target: &Scalar,
        ty: &DataType,
        scalar_map: &HashMap<Scalar, u64>,
    ) -> Result<FilterEvalResult> {
        if !self.filter_schema.has_field(filter_column)
            || !Xor8Filter::supported_type(ty)
            || target.is_null()
        {
            // The column doesn't have a filter.
            return Ok(FilterEvalResult::Uncertain);
        }

        let idx = self.filter_schema.index_of(filter_column)?;
        let filter = &self.filters[idx];

        let contains = if self.version == V2BloomBlock::VERSION {
            let data_value = scalar_to_datavalue(target);
            filter.contains(&data_value)
        } else {
            scalar_map
                .get(target)
                .map_or(true, |digest| filter.contains_digest(*digest))
        };

        if contains {
            Ok(FilterEvalResult::Uncertain)
        } else {
            Ok(FilterEvalResult::MustFalse)
        }
    }

    pub fn supported_type(data_type: &TableDataType) -> bool {
        let data_type = DataType::from(data_type);
        Xor8Filter::supported_type(&data_type)
    }

    /// Checks if the average length of a string column exceeds 256 bytes.
    /// If it does, the bloom index for the column will not be established.
    fn check_large_string(column: &Column) -> bool {
        if let Column::String(v) = &column {
            let bytes_per_row = v.data().len() / v.len().max(1);
            if bytes_per_row > 256 {
                return true;
            }
        }
        false
    }
}

fn visit_expr_column_eq_constant(
    expr: &mut Expr<String>,
    visitor: &mut impl FnMut(Span, &str, &Scalar, &DataType, &DataType) -> Result<Option<Expr<String>>>,
) -> Result<()> {
    // Find patterns like `Column = <constant>`, `<constant> = Column`,
    // or `MapColumn[<key>] = <constant>`, `<constant> = MapColumn[<key>]`
    match expr {
        Expr::FunctionCall {
            span,
            id,
            args,
            return_type,
            ..
        } if id.name() == "eq" => match args.as_slice() {
            [
                Expr::ColumnRef {
                    id,
                    data_type: column_type,
                    ..
                },
                Expr::Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                },
            ]
            | [
                Expr::Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                },
                Expr::ColumnRef {
                    id,
                    data_type: column_type,
                    ..
                },
            ] => {
                // decimal don't respect datatype equal
                // debug_assert_eq!(scalar_type, column_type);
                // If the visitor returns a new expression, then replace with the current expression.
                if scalar_type == column_type {
                    if let Some(new_expr) = visitor(*span, id, scalar, column_type, return_type)? {
                        *expr = new_expr;

                        return Ok(());
                    }
                }
            }
            [
                Expr::FunctionCall { id, args, .. },
                Expr::Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                },
            ]
            | [
                Expr::Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                },
                Expr::FunctionCall { id, args, .. },
            ] => {
                if id.name() == "get" {
                    if let Some(new_expr) =
                        visit_map_column(*span, args, scalar, scalar_type, return_type, visitor)?
                    {
                        *expr = new_expr;
                        return Ok(());
                    }
                }
            }
            [
                Expr::Cast {
                    expr:
                        box Expr::FunctionCall {
                            id,
                            args,
                            return_type,
                            ..
                        },
                    dest_type,
                    ..
                },
                Expr::Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                },
            ]
            | [
                Expr::Constant {
                    scalar,
                    data_type: scalar_type,
                    ..
                },
                Expr::Cast {
                    expr:
                        box Expr::FunctionCall {
                            id,
                            args,
                            return_type,
                            ..
                        },
                    dest_type,
                    ..
                },
            ] => {
                if id.name() == "get" {
                    // Only support cast variant value in map to string value
                    if return_type.remove_nullable() != DataType::Variant
                        || dest_type.remove_nullable() != DataType::String
                    {
                        return Ok(());
                    }
                    if let Some(new_expr) =
                        visit_map_column(*span, args, scalar, scalar_type, return_type, visitor)?
                    {
                        *expr = new_expr;
                        return Ok(());
                    }
                }
            }
            _ => (),
        },
        _ => (),
    }

    // Otherwise, rewrite sub expressions.
    match expr {
        Expr::Cast { expr, .. } => {
            visit_expr_column_eq_constant(expr, visitor)?;
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                visit_expr_column_eq_constant(arg, visitor)?;
            }
        }
        _ => (),
    }

    Ok(())
}

fn visit_map_column(
    span: Span,
    args: &[Expr<String>],
    scalar: &Scalar,
    scalar_type: &DataType,
    return_type: &DataType,
    visitor: &mut impl FnMut(Span, &str, &Scalar, &DataType, &DataType) -> Result<Option<Expr<String>>>,
) -> Result<Option<Expr<String>>> {
    match &args[0] {
        Expr::ColumnRef { id, data_type, .. }
        | Expr::Cast {
            expr: box Expr::ColumnRef { id, data_type, .. },
            ..
        } => {
            if let DataType::Map(box inner_ty) = data_type.remove_nullable() {
                let val_type = match inner_ty {
                    DataType::Tuple(kv_tys) => kv_tys[1].clone(),
                    _ => unreachable!(),
                };
                // Only JSON value of string type have bloom index.
                if val_type.remove_nullable() == DataType::Variant {
                    if scalar_type.remove_nullable() != DataType::String {
                        return Ok(None);
                    }
                } else if val_type.remove_nullable() != scalar_type.remove_nullable() {
                    return Ok(None);
                }
                return visitor(span, id, scalar, scalar_type, return_type);
            }
        }
        _ => {}
    }
    Ok(None)
}
