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

use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::map::KvColumn;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
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
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
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

    registry.register_3_arg_core::<EmptyMapType, GenericType<0>, GenericType<1>, MapType<GenericType<0>, GenericType<1>>, _, _>(
        "map_insert",
        |_, _, insert_key_domain, insert_value_domain| {
            FunctionDomain::Domain(Some((
                insert_key_domain.clone(),
                insert_value_domain.clone(),
            )))
        },
        |_, key, value, ctx| {
            let key_type = &ctx.generics[0];
            if !key_type.is_boolean()
                && !key_type.is_string()
                && !key_type.is_numeric()
                && !key_type.is_decimal()
                && !key_type.is_date_or_date_time()
            {
                ctx.set_error(0, format!("map keys can not be {}", key_type));
            }

            let mut b = ArrayType::create_builder(1, ctx.generics);
            b.put_item((key.into_scalar().unwrap(), value.into_scalar().unwrap()));
            b.commit_row();
            return Value::Scalar(MapType::build_scalar(b));
        },
    );

    registry.register_3_arg_core::<NullableType<MapType<GenericType<0>, GenericType<1>>>, GenericType<0>, GenericType<1>, MapType<GenericType<0>, GenericType<1>>, _, _>(
        "map_insert",
        |_, source_domain, insert_key_domain, insert_value_domain| {
            FunctionDomain::Domain(match source_domain.has_null {
                true => Some((
                    insert_key_domain.clone(),
                    insert_value_domain.clone(),
                )),
                false => source_domain.value.as_ref().map(|v| {
                    let a = v.clone().unwrap();
                    (a.0.clone(), a.1.clone())
                }),
            })
        },
        vectorize_with_builder_3_arg::<
            NullableType<MapType<GenericType<0>, GenericType<1>>>,
            GenericType<0>,
            GenericType<1>,
            MapType<GenericType<0>, GenericType<1>>,
        >(|source, key, value, output, ctx| {
            match source {
                Some(source) => {
                    output.append_column(&build_new_map(&source, key, value, ctx));
                },
                None => {
                    let mut b = ArrayType::create_builder(1, ctx.generics);
                    b.put_item((key.clone(), value.clone()));
                    b.commit_row();
                    output.append_column(&b.build());
                },
            };
        }),
    );

    registry.register_passthrough_nullable_3_arg(
        "map_insert",
        |_, domain1, key_domain, value_domain| {
            FunctionDomain::Domain(match (domain1, key_domain, value_domain) {
                (Some((key_domain, val_domain)), insert_key_domain, insert_value_domain) => Some((
                    key_domain.merge(insert_key_domain),
                    val_domain.merge(insert_value_domain),
                )),
                (None, _, _) => None,
            })
        },
        vectorize_with_builder_3_arg::<
            MapType<GenericType<0>, GenericType<1>>,
            GenericType<0>,
            GenericType<1>,
            MapType<GenericType<0>, GenericType<1>>,
        >(|source, key, value, output, ctx| {
            // default behavior is to insert new key-value pair, and if the key already exists, update the value.
            output.append_column(&build_new_map(&source, key, value, ctx));
        }),
    );

    // grammar: map_insert(map, insert_key, insert_value, allow_update)
    registry.register_passthrough_nullable_4_arg(
        "map_insert",
        |_, domain1, key_domain, value_domain, _| {
            FunctionDomain::Domain(match (domain1, key_domain, value_domain) {
                (Some((key_domain, val_domain)), insert_key_domain, insert_value_domain) => Some((
                    key_domain.merge(insert_key_domain),
                    val_domain.merge(insert_value_domain),
                )),
                (None, _, _) => None,
            })
        },
        vectorize_with_builder_4_arg::<
            MapType<GenericType<0>, GenericType<1>>,
            GenericType<0>,
            GenericType<1>,
            BooleanType,
            MapType<GenericType<0>, GenericType<1>>,
        >(|source, key, value, allow_update, output, ctx| {
            let duplicate_key = source.iter().any(|(k, _)| k == key);
            // if duplicate_key is true and allow_update is false, return the original map
            if duplicate_key && !allow_update {
                let mut new_builder = ArrayType::create_builder(source.len(), ctx.generics);
                source
                    .iter()
                    .for_each(|(k, v)| new_builder.put_item((k.clone(), v.clone())));
                new_builder.commit_row();
                output.append_column(&new_builder.build());
                return;
            }

            output.append_column(&build_new_map(&source, key, value, ctx));
        }),
    );

    fn build_new_map(
        source: &KvColumn<GenericType<0>, GenericType<1>>,
        insert_key: ScalarRef,
        insert_value: ScalarRef,
        ctx: &EvalContext,
    ) -> ArrayColumn<KvPair<GenericType<0>, GenericType<1>>> {
        let duplicate_key = source.iter().any(|(k, _)| k == insert_key);
        let mut new_map = ArrayType::create_builder(source.len() + 1, ctx.generics);
        for (k, v) in source.iter() {
            if k == insert_key {
                new_map.put_item((k.clone(), insert_value.clone()));
                continue;
            }
            new_map.put_item((k.clone(), v.clone()));
        }
        if !duplicate_key {
            new_map.put_item((insert_key.clone(), insert_value.clone()));
        }
        new_map.commit_row();

        new_map.build()
    }
}
