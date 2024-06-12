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

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::EmptyMapType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::ValueType;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::Column;
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
use databend_common_hashtable::StackHashSet;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

pub fn register(registry: &mut FunctionRegistry) {
    registry
        .register_passthrough_nullable_2_arg::<EmptyArrayType, EmptyArrayType, EmptyMapType, _, _>(
            "map",
            |_, _, _| FunctionDomain::Full,
            |_, _, _| Value::Scalar(()),
        );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<1>>, MapType<GenericType<0>, GenericType<1>>, _, _>(
        "map",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<1>>, MapType<GenericType<0>, GenericType<1>>>(
            |keys, vals, output, ctx| {
                let key_type = &ctx.generics[0];
                if !key_type.is_boolean()
                        && !key_type.is_string()
                        && !key_type.is_numeric()
                        && !key_type.is_decimal()
                        && !key_type.is_date_or_date_time() {
                    ctx.set_error(output.len(), format!("map keys can not be {}", key_type));
                } else if keys.len() != vals.len() {
                    ctx.set_error(output.len(), format!(
                        "key list has a different size from value list ({} keys, {} values)",
                        keys.len(), vals.len()
                    ));
                } else if keys.len() <= 1 {
                    for idx in 0..keys.len() {
                        let key = unsafe { keys.index_unchecked(idx) };
                        let val = unsafe { vals.index_unchecked(idx) };
                        output.put_item((key, val));
                    }
                } else {
                    let mut set: StackHashSet<u128, 16> =
                        StackHashSet::with_capacity(keys.len());
                    for idx in 0..keys.len() {
                        let key = unsafe { keys.index_unchecked(idx) };
                        let mut hasher = SipHasher24::new();
                        key.hash(&mut hasher);
                        let hash128 = hasher.finish128();
                        let hash_key = hash128.into();
                        if set.contains(&hash_key) {
                            ctx.set_error(output.len(), "map keys have to be unique");
                            break;
                        }
                        let _ = set.set_insert(hash_key);
                        let val = unsafe { vals.index_unchecked(idx) };
                        output.put_item((key, val));
                    }
                }
                output.commit_row();
            }
        ),
    );

    registry.register_2_arg_core::<NullableType<EmptyMapType>, NullableType<GenericType<0>>, NullType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_2_arg_core::<NullableType<MapType<GenericType<0>, NullType>>, NullableType<GenericType<0>>, NullType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_combine_nullable_2_arg::<MapType<GenericType<0>, NullableType<GenericType<1>>>, GenericType<0>, GenericType<1>, _, _>(
        "get",
        |_, domain, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: true,
                value: domain.as_ref().and_then(|(_, val_domain)| val_domain.value.clone())
            })
        },
        vectorize_with_builder_2_arg::<MapType<GenericType<0>, NullableType<GenericType<1>>>, GenericType<0>, NullableType<GenericType<1>>>(
            |map, key, output, _| {
                for (k, v) in map.iter() {
                    if k == key {
                        match v {
                            Some(v) => output.push(v),
                            None => output.push_null()
                        }
                        return
                    }
                }
                output.push_null()
            }
        ),
    );

    registry.register_1_arg_core::<EmptyMapType, EmptyArrayType, _, _>(
        "map_keys",
        |_, _| FunctionDomain::Full,
        |_, _| Value::Scalar(()),
    );

    registry.register_passthrough_nullable_1_arg::<MapType<GenericType<0>, GenericType<1>>, ArrayType<GenericType<0>>, _, _>(
        "map_keys",
        |_, domain| {
            FunctionDomain::Domain(
                domain.clone().map(|(key_domain, _)| key_domain.clone())
            )
        },
        vectorize_1_arg::<MapType<GenericType<0>, GenericType<1>>, ArrayType<GenericType<0>>>(
            |map, _| map.keys
        ),
    );

    registry.register_1_arg_core::<EmptyMapType, EmptyArrayType, _, _>(
        "map_values",
        |_, _| FunctionDomain::Full,
        |_, _| Value::Scalar(()),
    );

    registry.register_passthrough_nullable_1_arg::<MapType<GenericType<0>, GenericType<1>>, ArrayType<GenericType<1>>, _, _>(
        "map_values",
        |_, domain| {
            FunctionDomain::Domain(
                domain.clone().map(|(_, val_domain)| val_domain.clone())
            )
        },
        vectorize_1_arg::<MapType<GenericType<0>, GenericType<1>>, ArrayType<GenericType<1>>>(
            |map, _| map.values
        ),
    );

    registry.register_2_arg::<EmptyMapType, EmptyMapType, EmptyMapType, _, _>(
        "map_cat",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| (),
    );

    registry.register_passthrough_nullable_2_arg(
        "map_cat",
        |_, domain1, domain2| {
            FunctionDomain::Domain(match (domain1, domain2) {
                (Some((key_domain1, val_domain1)), Some((key_domain2, val_domain2))) => Some((
                    key_domain1.merge(key_domain2),
                    val_domain1.merge(val_domain2),
                )),
                (Some(domain1), None) => Some(domain1).cloned(),
                (None, Some(domain2)) => Some(domain2).cloned(),
                (None, None) => None,
            })
        },
        vectorize_with_builder_2_arg::<
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
        >(|lhs, rhs, output_map, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output_map.len()) {
                    output_map.push_default();
                    return;
                }
            }

            let mut concatenated_map_builder =
                ArrayType::create_builder(lhs.len() + rhs.len(), ctx.generics);
            let mut detect_dup_keys = HashSet::new();

            for (lhs_key, lhs_value) in lhs.iter() {
                if let Some((_, rhs_value)) = rhs.iter().find(|(rhs_key, _)| lhs_key == *rhs_key) {
                    detect_dup_keys.insert(lhs_key.clone());
                    concatenated_map_builder.put_item((lhs_key.clone(), rhs_value.clone()));
                } else {
                    concatenated_map_builder.put_item((lhs_key.clone(), lhs_value.clone()));
                }
            }

            for (rhs_key, rhs_value) in rhs.iter() {
                if !detect_dup_keys.contains(&rhs_key) {
                    concatenated_map_builder.put_item((rhs_key, rhs_value));
                }
            }

            concatenated_map_builder.commit_row();
            output_map.append_column(&concatenated_map_builder.build());
        }),
    );

    registry.register_1_arg_core::<EmptyMapType, NumberType<u8>, _, _>(
        "map_size",
        |_, _| FunctionDomain::Domain(SimpleDomain { min: 0, max: 0 }),
        |_, _| Value::Scalar(0u8),
    );

    registry.register_1_arg::<MapType<GenericType<0>, GenericType<1>>, NumberType<u64>, _, _>(
        "map_size",
        |_, _| FunctionDomain::Full,
        |map, _| map.len() as u64,
    );

    registry.register_2_arg_core::<EmptyMapType, GenericType<0>, BooleanType, _, _>(
        "map_contains_key",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(false),
    );

    registry
        .register_2_arg::<MapType<GenericType<0>, GenericType<1>>, GenericType<0>, BooleanType, _, _>(
            "map_contains_key",
            |_, _, _| FunctionDomain::Full,
            |map, key, _| {
                map.iter()
                    .any(|(k, _)| k == key)
            },
        );

    registry.register_function_factory("map_pick", |_, args_type: &[DataType]| {
        if args_type.len() < 2 {
            return None;
        }

        if !matches!(args_type[0], DataType::Map(_) | DataType::EmptyMap) {
            return None;
        }

        let inner_key_type = match args_type.first() {
            Some(DataType::Map(m)) => m.as_tuple().map(|tuple| &tuple[0]),
            _ => None,
        };
        let key_match = match args_type.len() {
            2 => args_type.get(1).map_or(false, |t| match t {
                DataType::Array(_) => inner_key_type.map_or(false, |key_type| {
                    t.as_array()
                        .map_or(false, |array| array.as_ref() == key_type)
                }),
                DataType::EmptyArray => false,
                _ => false,
            }),
            _ => args_type.iter().skip(1).all(|arg_type| {
                inner_key_type.map_or_else(
                    || {
                        matches!(
                            arg_type,
                            DataType::String
                                | DataType::Number(_)
                                | DataType::Decimal(_)
                                | DataType::Date
                                | DataType::Timestamp
                        )
                    },
                    |key_type| arg_type == key_type,
                )
            }),
        };
        if !key_match {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "map_pick".to_string(),
                args_type: args_type.to_vec(),
                return_type: args_type[0].clone(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |_, _| FunctionDomain::Full),
                eval: Box::new(map_pick_fn_vec),
            },
        }))
    });

    fn map_pick_fn_vec(args: &[ValueRef<AnyType>], _: &mut EvalContext) -> Value<AnyType> {
        let len = args.iter().find_map(|arg| match arg {
            ValueRef::Column(col) => Some(col.len()),
            _ => None,
        });

        let source_data_type = match args.first().unwrap() {
            ValueRef::Scalar(s) => s.infer_data_type(),
            ValueRef::Column(c) => c.data_type(),
        };

        let source_map = match &args[0] {
            ValueRef::Scalar(s) => match s {
                ScalarRef::Map(cols) => {
                    KvPair::<GenericType<0>, GenericType<1>>::try_downcast_column(cols).unwrap()
                }
                ScalarRef::EmptyMap => {
                    KvPair::<GenericType<0>, GenericType<1>>::try_downcast_column(
                        &Column::EmptyMap { len: 0 },
                    )
                    .unwrap()
                }
                _ => unreachable!(),
            },
            ValueRef::Column(Column::Map(c)) => {
                KvPair::<GenericType<0>, GenericType<1>>::try_downcast_column(&c.values).unwrap()
            }
            _ => unreachable!(),
        };

        let mut builder: ArrayColumnBuilder<KvPair<GenericType<0>, GenericType<1>>> =
            ArrayType::create_builder(
                args.len() - 1,
                source_data_type.as_map().unwrap().as_tuple().unwrap(),
            );
        let select_keys = match &args[1] {
            ValueRef::Scalar(ScalarRef::Array(arr)) if args.len() == 2 => {
                arr.iter().collect::<Vec<_>>()
            }
            _ => args[1..]
                .iter()
                .map(|arg| arg.as_scalar().unwrap().clone())
                .collect::<Vec<_>>(),
        };
        for key_arg in select_keys {
            if let Some((k, v)) = source_map.iter().find(|(k, _)| k == &key_arg) {
                builder.put_item((k.clone(), v.clone()));
            }
        }
        builder.commit_row();

        match len {
            Some(_) => Value::Column(Column::Map(Box::new(builder.build().upcast()))),
            _ => {
                let scalar_builder = builder.build_scalar();
                Value::Scalar(Scalar::Map(Column::Tuple(vec![
                    scalar_builder.keys,
                    scalar_builder.values,
                ])))
            }
        }
    }

    registry.register_2_arg_core::<EmptyMapType, EmptyArrayType, EmptyMapType, _, _>(
        "map_pick",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_2_arg_core::<EmptyMapType, ArrayType<GenericType<0>>, EmptyMapType, _, _>(
        "map_pick",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );
}
