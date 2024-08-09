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

use databend_common_exception::ErrorCode;
use databend_common_expression::types::GeographyType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_makepoint", &["st_point"]);

    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType, _, _>(
        "st_makepoint",
        |_,_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType>(|longitude, latitude, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push_default();
                    return;
                }
            }
            let geog = GeographyType::point(*longitude, *latitude);
            if let Err(e) = geog.check() {
                ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                );
                builder.push_default();
            } else {
                builder.push(geog.as_ref())
            }
        })
    );
}
