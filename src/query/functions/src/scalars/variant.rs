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

use std::borrow::Cow;
use std::collections::HashSet;
use std::iter::once;
use std::sync::Arc;

use bstr::ByteSlice;
use chrono::Datelike;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::*;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::variant::cast_scalars_to_variants;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use jsonb::array_length;
use jsonb::as_bool;
use jsonb::as_f64;
use jsonb::as_i64;
use jsonb::as_str;
use jsonb::build_array;
use jsonb::build_object;
use jsonb::concat;
use jsonb::contains;
use jsonb::delete_by_index;
use jsonb::delete_by_keypath;
use jsonb::delete_by_name;
use jsonb::exists_all_keys;
use jsonb::exists_any_keys;
use jsonb::get_by_index;
use jsonb::get_by_keypath;
use jsonb::get_by_name;
use jsonb::get_by_path;
use jsonb::get_by_path_array;
use jsonb::get_by_path_first;
use jsonb::is_array;
use jsonb::is_boolean;
use jsonb::is_f64;
use jsonb::is_i64;
use jsonb::is_null;
use jsonb::is_object;
use jsonb::is_string;
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::JsonPath;
use jsonb::keypath::parse_key_paths;
use jsonb::object_keys;
use jsonb::parse_value;
use jsonb::path_exists;
use jsonb::path_match;
use jsonb::strip_nulls;
use jsonb::to_bool;
use jsonb::to_f64;
use jsonb::to_i64;
use jsonb::to_pretty_string;
use jsonb::to_str;
use jsonb::to_string;
use jsonb::to_u64;
use jsonb::type_of;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("json_object_keys", &["object_keys"]);
    registry.register_aliases("to_string", &["json_to_string"]);

    registry.register_passthrough_nullable_1_arg::<VariantType, VariantType, _, _>(
        "parse_json",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then parse.
            let val = to_string(s);
            match parse_value(val.as_bytes()) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    if ctx.func_ctx.disable_variant_check {
                        output.put_str("");
                    } else {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
            }
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, VariantType, _, _>(
        "parse_json",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            match parse_value(s.as_bytes()) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    if ctx.func_ctx.disable_variant_check {
                        output.put_str("");
                    } else {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
            }
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "try_parse_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then parse.
            let val = to_string(s);
            match parse_value(val.as_bytes()) {
                Ok(value) => {
                    output.validity.push(true);
                    value.write_to_vec(&mut output.builder.data);
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, VariantType, _, _>(
        "try_parse_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<VariantType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match parse_value(s.as_bytes()) {
                Ok(value) => {
                    output.validity.push(true);
                    value.write_to_vec(&mut output.builder.data);
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "check_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then check.
            let val = to_string(s);
            match parse_value(val.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(&e.to_string()),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, StringType, _, _>(
        "check_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<StringType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match parse_value(s.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(&e.to_string()),
            }
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<UInt32Type>, _, _>(
        "length",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<UInt32Type>>(|val, _| {
            val.and_then(|v| array_length(v).map(|v| v as u32))
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "json_object_keys",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(object_keys)
        }),
    );

    registry.register_function_factory("get_by_keypath", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_by_keypath".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(|args, ctx| get_by_keypath_fn(args, ctx, false)),
            },
        }))
    });

    registry.register_function_factory("get_by_keypath_string", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_by_keypath_string".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::String)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(|args, ctx| get_by_keypath_fn(args, ctx, true)),
            },
        }))
    });

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match get_by_name(val, name, false) {
                    Some(v) => {
                        output.push(&v);
                    }
                    None => {
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int64Type, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int64Type, NullableType<VariantType>>(
            |val, idx, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if idx < 0 || idx > i32::MAX.into() {
                    output.push_null();
                } else {
                    match get_by_index(val, idx as usize) {
                        Some(v) => {
                            output.push(&v);
                        }
                        None => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_ignore_case",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match get_by_name(val, name, true) {
                    Some(v) => output.push(&v),
                    None => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, StringType, _, _>(
        "get_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<StringType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match get_by_name(val, name, false) {
                    Some(v) => {
                        let json_str = cast_to_string(&v);
                        output.push(&json_str);
                    }
                    None => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int64Type, StringType, _, _>(
        "get_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int64Type, NullableType<StringType>>(
            |val, idx, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if idx < 0 || idx > i32::MAX.into() {
                    output.push_null();
                } else {
                    match get_by_index(val, idx as usize) {
                        Some(v) => {
                            let json_str = cast_to_string(&v);
                            output.push(&json_str);
                        }
                        None => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "json_path_query_array",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path.as_bytes()) {
                    Ok(json_path) => {
                        match get_by_path_array(
                            val,
                            json_path,
                            &mut output.builder.data,
                            &mut output.builder.offsets,
                        ) {
                            Ok(()) => {
                                if output.builder.offsets.len() == output.len() + 1 {
                                    output.push_null();
                                } else {
                                    output.validity.push(true);
                                }
                            }
                            Err(_) => {
                                ctx.set_error(
                                    output.len(),
                                    format!("Invalid JSONB value '0x{}'", hex::encode(val)),
                                );
                                output.push_null();
                            }
                        }
                    }
                    Err(_) => {
                        ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "json_path_query_first",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path.as_bytes()) {
                    Ok(json_path) => {
                        match get_by_path_first(
                            val,
                            json_path,
                            &mut output.builder.data,
                            &mut output.builder.offsets,
                        ) {
                            Ok(()) => {
                                if output.builder.offsets.len() == output.len() + 1 {
                                    output.push_null();
                                } else {
                                    output.validity.push(true);
                                }
                            }
                            Err(_) => {
                                ctx.set_error(
                                    output.len(),
                                    format!("Invalid JSONB value '0x{}'", hex::encode(val)),
                                );
                                output.push_null();
                            }
                        }
                    }
                    Err(_) => {
                        ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_function_factory("json_path_match", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_path_match".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Boolean)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(|args, ctx| path_predicate_fn(args, ctx, path_match)),
            },
        }))
    });

    registry.register_function_factory("json_path_exists", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_path_exists".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Boolean)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(|args, ctx| {
                    path_predicate_fn(args, ctx, |json, path| path_exists(json, path))
                }),
            },
        }))
    });

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_path",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path.as_bytes()) {
                    Ok(json_path) => {
                        match get_by_path(
                            val,
                            json_path,
                            &mut output.builder.data,
                            &mut output.builder.offsets,
                        ) {
                            Ok(()) => {
                                if output.builder.offsets.len() == output.len() + 1 {
                                    output.push_null();
                                } else {
                                    output.validity.push(true);
                                }
                            }
                            Err(_) => {
                                ctx.set_error(
                                    output.len(),
                                    format!("Invalid JSONB value '0x{}'", hex::encode(val)),
                                );
                                output.push_null();
                            }
                        }
                    }
                    Err(_) => {
                        ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "json_extract_path_text",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<StringType>>(
            |s, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_value(s.as_bytes()) {
                    Ok(val) => {
                        let mut buf = Vec::new();
                        val.write_to_vec(&mut buf);
                        match parse_json_path(path.as_bytes()) {
                            Ok(json_path) => {
                                let mut out_buf = Vec::new();
                                let mut out_offsets = Vec::new();
                                match get_by_path(&buf, json_path, &mut out_buf, &mut out_offsets) {
                                    Ok(()) => {
                                        if out_offsets.is_empty() {
                                            output.push_null();
                                        } else {
                                            let json_str = cast_to_string(&out_buf);
                                            output.push(&json_str);
                                        }
                                    }
                                    Err(_) => {
                                        ctx.set_error(
                                            output.len(),
                                            format!("Invalid JSONB value '0x{}'", hex::encode(buf)),
                                        );
                                        output.push_null();
                                    }
                                }
                            }
                            Err(_) => {
                                ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
                                output.push_null();
                            }
                        }
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "as_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_bool(v) {
                Some(val) => output.push(val),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Int64Type, _, _>(
        "as_integer",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Int64Type>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_i64(v) {
                Some(val) => output.push(val),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Float64Type, _, _>(
        "as_float",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Float64Type>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_f64(v) {
                Some(val) => output.push(val.into()),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "as_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_str(v) {
                Some(val) => output.push(&val),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_array",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            if is_array(v) {
                output.push(v.as_bytes());
            } else {
                output.push_null()
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_object",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            if is_object(v) {
                output.push(v.as_bytes());
            } else {
                output.push_null()
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_null_value",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_null(val));
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_boolean(val));
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_integer",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_i64(val));
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_float",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_f64(val));
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_string(val));
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_array",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_array(val));
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_object",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            output.push(is_object(val));
        }),
    );

    registry.register_function_factory("to_variant", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        let return_type = if args_type[0].is_nullable_or_null() {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_variant".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, args_domain| match &args_domain[0] {
                    Domain::Nullable(nullable_domain) => {
                        FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                            has_null: nullable_domain.has_null,
                            value: Some(Box::new(Domain::Undefined)),
                        }))
                    }
                    _ => FunctionDomain::Domain(Domain::Undefined),
                }),
                eval: Box::new(|args, ctx| match &args[0] {
                    ValueRef::Scalar(scalar) => match scalar {
                        ScalarRef::Null => Value::Scalar(Scalar::Null),
                        _ => {
                            let mut buf = Vec::new();
                            cast_scalar_to_variant(scalar.clone(), ctx.func_ctx.tz, &mut buf);
                            Value::Scalar(Scalar::Variant(buf))
                        }
                    },
                    ValueRef::Column(col) => {
                        let validity = match col {
                            Column::Null { len } => Some(Bitmap::new_constant(false, *len)),
                            Column::Nullable(box ref nullable_column) => {
                                Some(nullable_column.validity.clone())
                            }
                            _ => None,
                        };
                        let new_col = cast_scalars_to_variants(col.iter(), ctx.func_ctx.tz);
                        if let Some(validity) = validity {
                            Value::Column(NullableColumn::new_column(
                                Column::Variant(new_col),
                                validity,
                            ))
                        } else {
                            Value::Column(Column::Variant(new_col))
                        }
                    }
                }),
            },
        }))
    });

    registry.register_combine_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "try_to_variant",
        |_, domain| {
            let has_null = match domain {
                Domain::Nullable(nullable_domain) => nullable_domain.has_null,
                _ => false,
            };
            FunctionDomain::Domain(NullableDomain {
                has_null,
                value: Some(Box::new(())),
            })
        },
        |val, ctx| match val {
            ValueRef::Scalar(scalar) => match scalar {
                ScalarRef::Null => Value::Scalar(None),
                _ => {
                    let mut buf = Vec::new();
                    cast_scalar_to_variant(scalar, ctx.func_ctx.tz, &mut buf);
                    Value::Scalar(Some(buf))
                }
            },
            ValueRef::Column(col) => {
                let validity = match col {
                    Column::Null { len } => Bitmap::new_constant(false, len),
                    Column::Nullable(box ref nullable_column) => nullable_column.validity.clone(),
                    _ => Bitmap::new_constant(true, col.len()),
                };
                let new_col = cast_scalars_to_variants(col.iter(), ctx.func_ctx.tz);
                Value::Column(NullableColumn::new(new_col, validity))
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "to_boolean",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match to_bool(val) {
                Ok(value) => output.push(value),
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                    output.push(false);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "try_to_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match to_bool(val) {
                    Ok(value) => {
                        output.push(value);
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let json_str = cast_to_string(val);
            output.put_str(&json_str);
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "try_to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let json_str = cast_to_string(val);
                output.push(&json_str);
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, DateType, _, _>(
        "to_date",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, DateType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }
            let val = as_str(val);
            match val {
                Some(val) => match string_to_date(
                    val.as_bytes(),
                    ctx.func_ctx.tz.tz,
                    ctx.func_ctx.enable_dst_hour_fix,
                ) {
                    Ok(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("unable to cast to type `DATE`. {}", e),
                        );
                        output.push(0);
                    }
                },
                None => {
                    ctx.set_error(output.len(), "unable to cast to type `DATE`");
                    output.push(0);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, DateType, _, _>(
        "try_to_date",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            let val = as_str(val);
            match val {
                Some(val) => match string_to_date(
                    val.as_bytes(),
                    ctx.func_ctx.tz.tz,
                    ctx.func_ctx.enable_dst_hour_fix,
                ) {
                    Ok(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    Err(_) => output.push_null(),
                },
                None => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "to_timestamp",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, TimestampType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }
            let val = as_str(val);
            match val {
                Some(val) => match string_to_timestamp(
                    val.as_bytes(),
                    ctx.func_ctx.tz.tz,
                    ctx.func_ctx.enable_dst_hour_fix,
                ) {
                    Ok(ts) => output.push(ts.timestamp_micros()),
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("unable to cast to type `TIMESTAMP`. {}", e),
                        );
                        output.push(0);
                    }
                },
                None => {
                    ctx.set_error(output.len(), "unable to cast to type `TIMESTAMP`");
                    output.push(0);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let val = as_str(val);
                match val {
                    Some(val) => match string_to_timestamp(
                        val.as_bytes(),
                        ctx.func_ctx.tz.tz,
                        ctx.func_ctx.enable_dst_hour_fix,
                    ) {
                        Ok(ts) => output.push(ts.timestamp_micros()),
                        Err(_) => {
                            output.push_null();
                        }
                    },
                    None => {
                        output.push_null();
                    }
                }
            },
        ),
    );

    for dest_type in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match dest_type {
            NumberDataType::NUM_TYPE => {
                let name = format!("to_{dest_type}").to_lowercase();
                registry
                    .register_passthrough_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<VariantType, NumberType<NUM_TYPE>>(
                            move |val, output, ctx| {
                                if let Some(validity) = &ctx.validity {
                                    if !validity.get_bit(output.len()) {
                                        output.push(NUM_TYPE::default());
                                        return;
                                    }
                                }
                                type Native = <NUM_TYPE as Number>::Native;

                                let value: Option<Native> = if dest_type.is_float() {
                                    to_f64(val).ok().and_then(num_traits::cast::cast)
                                } else if dest_type.is_signed() {
                                    to_i64(val).ok().and_then(num_traits::cast::cast)
                                } else {
                                    to_u64(val).ok().and_then(num_traits::cast::cast)
                                };
                                match value {
                                    Some(value) => output.push(value.into()),
                                    None => {
                                        ctx.set_error(
                                            output.len(),
                                            format!("unable to cast to type {dest_type}",),
                                        );
                                        output.push(NUM_TYPE::default());
                                    }
                                }
                            },
                        ),
                    );

                let name = format!("try_to_{dest_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            VariantType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(move |val, output, ctx| {
                            if let Some(validity) = &ctx.validity {
                                if !validity.get_bit(output.len()) {
                                    output.push_null();
                                    return;
                                }
                            }
                            if dest_type.is_float() {
                                if let Ok(value) = to_f64(val) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else if dest_type.is_signed() {
                                if let Ok(value) = to_i64(val) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else {
                                if let Ok(value) = to_u64(val) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            }
                        }),
                    );
            }
        });
    }

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "json_pretty",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let s = to_pretty_string(val);
            output.put_str(&s);
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg(
        "json_strip_nulls",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            if let Err(err) = strip_nulls(val, &mut output.data) {
                ctx.set_error(output.len(), err.to_string());
            };
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg(
        "concat",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, VariantType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                if let Err(err) = concat(left, right, &mut output.data) {
                    ctx.set_error(output.len(), err.to_string());
                };
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, Int32Type, VariantType>(
            |val, index, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                if let Err(err) = delete_by_index(val, index, &mut output.data) {
                    ctx.set_error(output.len(), err.to_string());
                };
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, VariantType>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                if let Err(err) = delete_by_name(val, name, &mut output.data) {
                    ctx.set_error(output.len(), err.to_string());
                };
                output.commit_row();
            },
        ),
    );

    registry.register_function_factory("delete_by_keypath", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "delete_by_keypath".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(delete_by_keypath_fn),
            },
        }))
    });

    registry.register_function_factory("json_insert", |_, args_type| {
        if args_type.len() != 3 && args_type.len() != 4 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
            || (args_type[2].remove_nullable() != DataType::Variant
                && args_type[2] != DataType::Null)
        {
            return None;
        }
        if args_type.len() == 4 && args_type[3] != DataType::Boolean {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_insert".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(insert_by_keypath_fn),
            },
        }))
    });

    registry.register_function_factory("json_array_distinct", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null) {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_array_distinct".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_array_distinct_fn),
            },
        }))
    });

    registry.register_passthrough_nullable_1_arg(
        "json_typeof",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            match type_of(val) {
                Ok(result) => output.put_str(result),
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                }
            };
            output.commit_row();
        }),
    );

    registry.register_function_factory("json_object", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_object_fn),
            },
        }))
    });

    registry.register_function_factory("try_json_object", |_, args_type| {
        let f = Function {
            signature: FunctionSignature {
                name: "try_json_object".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(json_object_fn),
            },
        };
        Some(Arc::new(f.error_to_null()))
    });

    registry.register_function_factory("json_object_keep_null", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object_keep_null".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_object_keep_null_fn),
            },
        }))
    });

    registry.register_function_factory("try_json_object_keep_null", |_, args_type| {
        let f = Function {
            signature: FunctionSignature {
                name: "try_json_object_keep_null".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(json_object_keep_null_fn),
            },
        };
        Some(Arc::new(f.error_to_null()))
    });

    registry.register_function_factory("json_array", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_array".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_array_fn),
            },
        }))
    });

    registry.register_passthrough_nullable_2_arg(
        "json_contains_in_left",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, VariantType, BooleanType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let result = contains(left, right);
                output.push(result);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_contains_in_right",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, VariantType, BooleanType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let result = contains(right, left);
                output.push(result);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_exists_any_keys",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, ArrayType<StringType>, BooleanType>(
            |val, keys, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let result = exists_any_keys(val, keys.iter().map(|k| k.as_bytes()));
                output.push(result);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_exists_all_keys",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, ArrayType<StringType>, BooleanType>(
            |val, keys, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let result = exists_all_keys(val, keys.iter().map(|k| k.as_bytes()));
                output.push(result);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_exists_key",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, BooleanType>(
            |val, key, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let result = exists_all_keys(val, once(key.as_bytes()));
                output.push(result);
            },
        ),
    );
}

fn json_array_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let (columns, len) = prepare_args_columns(args, ctx);
    let cap = len.unwrap_or(1);
    let mut builder = BinaryColumnBuilder::with_capacity(cap, cap * 50);
    let mut items = Vec::with_capacity(columns.len());

    for idx in 0..cap {
        items.clear();
        for column in &columns {
            let v = unsafe { column.index_unchecked(idx) };
            let mut val = vec![];
            cast_scalar_to_variant(v, ctx.func_ctx.tz, &mut val);
            items.push(val);
        }
        if let Err(err) = build_array(items.iter().map(|b| &b[..]), &mut builder.data) {
            ctx.set_error(builder.len(), err.to_string());
        };
        builder.commit_row();
    }
    match len {
        Some(_) => Value::Column(Column::Variant(builder.build())),
        None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
    }
}

fn json_object_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    json_object_impl_fn(args, ctx, false)
}

fn json_object_keep_null_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    json_object_impl_fn(args, ctx, true)
}

fn json_object_impl_fn(
    args: &[ValueRef<AnyType>],
    ctx: &mut EvalContext,
    keep_null: bool,
) -> Value<AnyType> {
    let (columns, len) = prepare_args_columns(args, ctx);
    let cap = len.unwrap_or(1);
    let mut builder = BinaryColumnBuilder::with_capacity(cap, cap * 50);
    if columns.len() % 2 != 0 {
        for i in 0..cap {
            ctx.set_error(i, "The number of keys and values must be equal");
            builder.commit_row();
        }
    } else {
        let mut set = HashSet::new();
        let mut kvs = Vec::with_capacity(columns.len() / 2);
        for idx in 0..cap {
            set.clear();
            kvs.clear();
            let mut has_err = false;
            for i in (0..columns.len()).step_by(2) {
                let k = unsafe { columns[i].index_unchecked(idx) };
                if k == ScalarRef::Null {
                    continue;
                }
                let v = unsafe { columns[i + 1].index_unchecked(idx) };
                if v == ScalarRef::Null && !keep_null {
                    continue;
                }
                let key = match k {
                    ScalarRef::String(v) => v,
                    _ => {
                        has_err = true;
                        ctx.set_error(builder.len(), "Key must be a string value");
                        break;
                    }
                };
                if set.contains(&key) {
                    has_err = true;
                    ctx.set_error(builder.len(), "Keys have to be unique");
                    break;
                }
                set.insert(key);
                let mut val = vec![];
                cast_scalar_to_variant(v, ctx.func_ctx.tz, &mut val);
                kvs.push((key, val));
            }
            if !has_err {
                if let Err(err) =
                    build_object(kvs.iter().map(|(k, v)| (k, &v[..])), &mut builder.data)
                {
                    ctx.set_error(builder.len(), err.to_string());
                }
            }
            builder.commit_row();
        }
    }
    match len {
        Some(_) => Value::Column(Column::Variant(builder.build())),
        None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
    }
}

fn prepare_args_columns(
    args: &[ValueRef<AnyType>],
    ctx: &EvalContext,
) -> (Vec<Column>, Option<usize>) {
    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let mut columns = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let column = match arg {
            ValueRef::Column(column) => column.clone(),
            ValueRef::Scalar(s) => {
                let column_builder = ColumnBuilder::repeat(s, len, &ctx.generics[i]);
                column_builder.build()
            }
        };
        columns.push(column);
    }
    (columns, len_opt)
}

fn delete_by_keypath_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let scalar_keypath = match &args[1] {
        ValueRef::Scalar(ScalarRef::String(v)) => Some(parse_key_paths(v.as_bytes())),
        _ => None,
    };
    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut builder = BinaryColumnBuilder::with_capacity(len, len * 50);
    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let keypath = match &args[1] {
            ValueRef::Scalar(_) => Cow::Borrowed(&scalar_keypath),
            ValueRef::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                let path = match scalar {
                    ScalarRef::String(buf) => Some(parse_key_paths(buf.as_bytes())),
                    _ => None,
                };
                Cow::Owned(path)
            }
        };
        match keypath.as_ref() {
            Some(result) => match result {
                Ok(path) => {
                    let json_row = match &args[0] {
                        ValueRef::Scalar(scalar) => scalar.clone(),
                        ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    match json_row {
                        ScalarRef::Variant(json) => {
                            match delete_by_keypath(json, path.paths.iter(), &mut builder.data) {
                                Ok(_) => validity.push(true),
                                Err(err) => {
                                    ctx.set_error(builder.len(), err.to_string());
                                    validity.push(false);
                                }
                            }
                        }
                        _ => validity.push(false),
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    validity.push(false);
                }
            },
            None => validity.push(false),
        }
        builder.commit_row();
    }

    let validity: Bitmap = validity.into();

    match len_opt {
        Some(_) => Value::Column(Column::Variant(builder.build())).wrap_nullable(Some(validity)),
        None => {
            if !validity.get_bit(0) {
                Value::Scalar(Scalar::Null)
            } else {
                Value::Scalar(Scalar::Variant(builder.build_scalar()))
            }
        }
    }
}


fn insert_by_keypath_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let scalar_keypath = match &args[1] {
        ValueRef::Scalar(ScalarRef::String(v)) => Some(parse_key_paths(v.as_bytes())),
        _ => None,
    };

    let mut builder = BinaryColumnBuilder::with_capacity(len, len * 50);
    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let keypath = match &args[1] {
            ValueRef::Scalar(_) => Cow::Borrowed(&scalar_keypath),
            ValueRef::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                let path = match scalar {
                    ScalarRef::String(buf) => Some(parse_key_paths(buf.as_bytes())),
                    _ => None,
                };
                Cow::Owned(path)
            }
        };
        match keypath.as_ref() {
            Some(keypath) => match keypath {
                Ok(path) => {
                    let value = match &args[0] {
                        ValueRef::Scalar(scalar) => scalar.clone(),
                        ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    let new_value = match &args[2] {
                        ValueRef::Scalar(scalar) => scalar.clone(),
                        ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    let insert_after = if args.len() == 4 {
                        let insert_after_value = match &args[3] {
                            ValueRef::Scalar(scalar) => scalar.clone(),
                            ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
                        };
                        match insert_after_value {
                            ScalarRef::Boolean(v) => v,
                            _ => false,
                        }
                    } else {
                        false
                    };
                    match (value, new_value) {
                        (ScalarRef::Variant(value), ScalarRef::Variant(new_value)) => {
                            match jsonb::insert_by_keypath(value, path.paths.iter(), new_value, insert_after, &mut builder.data) {
                                Ok(_) => validity.push(true),
                                Err(err) => {
                                    ctx.set_error(builder.len(), err.to_string());
                                    validity.push(false);
                                }
                            }
                        }
                        (_, _) => validity.push(false),
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    validity.push(false);
                }
            },
            None => validity.push(false),
        }
        builder.commit_row();
    }

    let validity: Bitmap = validity.into();
    match len_opt {
        Some(_) => Value::Column(Column::Variant(builder.build())).wrap_nullable(Some(validity)),
        None => {
            if !validity.get_bit(0) {
                Value::Scalar(Scalar::Null)
            } else {
                Value::Scalar(Scalar::Variant(builder.build_scalar()))
            }
        }
    }
}


fn json_array_distinct_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut builder = BinaryColumnBuilder::with_capacity(len, len * 50);
    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let value = match &args[0] {
            ValueRef::Scalar(scalar) => scalar.clone(),
            ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
        };

        match value {
            ScalarRef::Variant(value) => {
                match jsonb::dedup(value, &mut builder.data) {
                    Ok(_) => validity.push(true),
                    Err(err) => {
                        ctx.set_error(builder.len(), err.to_string());
                        validity.push(false);
                    }
                }
            }
            _ => {
                validity.push(false);
            }
        }
        builder.commit_row();
    }

    let validity: Bitmap = validity.into();
    match len_opt {
        Some(_) => Value::Column(Column::Variant(builder.build())).wrap_nullable(Some(validity)),
        None => {
            if !validity.get_bit(0) {
                Value::Scalar(Scalar::Null)
            } else {
                Value::Scalar(Scalar::Variant(builder.build_scalar()))
            }
        }
    }
}

fn get_by_keypath_fn(
    args: &[ValueRef<AnyType>],
    ctx: &mut EvalContext,
    string_res: bool,
) -> Value<AnyType> {
    let scalar_keypath = match &args[1] {
        ValueRef::Scalar(ScalarRef::String(v)) => Some(parse_key_paths(v.as_bytes())),
        _ => None,
    };
    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut builder = if string_res {
        ColumnBuilder::String(StringColumnBuilder::with_capacity(len, len * 50))
    } else {
        ColumnBuilder::Variant(BinaryColumnBuilder::with_capacity(len, len * 50))
    };

    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let keypath = match &args[1] {
            ValueRef::Scalar(_) => Cow::Borrowed(&scalar_keypath),
            ValueRef::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                let path = match scalar {
                    ScalarRef::String(buf) => Some(parse_key_paths(buf.as_bytes())),
                    _ => None,
                };
                Cow::Owned(path)
            }
        };

        match keypath.as_ref() {
            Some(result) => match result {
                Ok(path) => {
                    let json_row = match &args[0] {
                        ValueRef::Scalar(scalar) => scalar.clone(),
                        ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    match json_row {
                        ScalarRef::Variant(json) => match get_by_keypath(json, path.paths.iter()) {
                            Some(res) => {
                                match &mut builder {
                                    ColumnBuilder::String(builder) => {
                                        let json_str = cast_to_string(&res);
                                        builder.put_str(&json_str);
                                    }
                                    ColumnBuilder::Variant(builder) => {
                                        builder.put_slice(&res);
                                    }
                                    _ => unreachable!(),
                                }
                                validity.push(true);
                            }
                            None => validity.push(false),
                        },
                        _ => validity.push(false),
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    validity.push(false);
                }
            },
            None => validity.push(false),
        }

        match &mut builder {
            ColumnBuilder::String(builder) => {
                builder.commit_row();
            }
            ColumnBuilder::Variant(builder) => {
                builder.commit_row();
            }
            _ => unreachable!(),
        }
    }

    let builder = ColumnBuilder::Nullable(Box::new(NullableColumnBuilder { builder, validity }));

    match len_opt {
        Some(_) => Value::Column(builder.build()),
        None => Value::Scalar(builder.build_scalar()),
    }
}

fn path_predicate_fn<'a, P>(
    args: &'a [ValueRef<AnyType>],
    ctx: &'a mut EvalContext,
    predicate: P,
) -> Value<AnyType>
where
    P: Fn(&'a [u8], JsonPath<'a>) -> Result<bool, jsonb::Error>,
{
    let scalar_jsonpath = match &args[1] {
        ValueRef::Scalar(ScalarRef::String(v)) => {
            let res = parse_json_path(v.as_bytes()).map_err(|_| format!("Invalid JSON Path '{v}'"));
            Some(res)
        }
        _ => None,
    };

    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut output = MutableBitmap::with_capacity(len);
    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let jsonpath = match &args[1] {
            ValueRef::Scalar(_) => scalar_jsonpath.clone(),
            ValueRef::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                match scalar {
                    ScalarRef::String(buf) => {
                        let res = parse_json_path(buf.as_bytes())
                            .map_err(|_| format!("Invalid JSON Path '{buf}'"));
                        Some(res)
                    }
                    _ => None,
                }
            }
        };
        match jsonpath {
            Some(result) => match result {
                Ok(path) => {
                    let json_row = match &args[0] {
                        ValueRef::Scalar(scalar) => scalar.clone(),
                        ValueRef::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    match json_row {
                        ScalarRef::Variant(json) => match predicate(json, path) {
                            Ok(r) => {
                                output.push(r);
                                validity.push(true);
                            }
                            Err(err) => {
                                ctx.set_error(output.len(), err.to_string());
                                output.push(false);
                                validity.push(false);
                            }
                        },
                        _ => {
                            output.push(false);
                            validity.push(false);
                        }
                    }
                }
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(false);
                    validity.push(false);
                }
            },
            None => {
                output.push(false);
                validity.push(false);
            }
        }
    }

    let validity: Bitmap = validity.into();

    match len_opt {
        Some(_) => Value::Column(Column::Boolean(output.into())).wrap_nullable(Some(validity)),
        None => {
            if !validity.get_bit(0) {
                Value::Scalar(Scalar::Null)
            } else {
                Value::Scalar(Scalar::Boolean(output.get(0)))
            }
        }
    }
}

// Extract string for string type, other types convert to JSON string.
fn cast_to_string(v: &[u8]) -> String {
    match to_str(v) {
        Ok(v) => v,
        Err(_) => to_string(v),
    }
}
