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

use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;

/// limits automatic type casting when loading data with a specified schema, applicable to formats like Parquet, ORC, Iceberg, and Delta.
///
/// # Operation:
///
/// - The check occurs after the schema is read but before the data is processed.
/// - This approach is designed to:
///   - Enhance User Experience: By identifying errors early, especially beneficial when loading large datasets.
///   - Optimize Schema Matching: Assist users in achieving a correct schema more swiftly and avoiding potential mistakes.
///
/// # Notes:
///
/// - A successful return does not guarantee load success; some casts depend on the actual data values.
/// - Users have the option to use casting functions in transformations to override these rules.
/// - This is distinct from the casting behavior in SQL statements like INSERT INTO <table> VALUES <values>, without an explicit FROM_TYPE.
/// - It differs from common_expression::can_auto_cast_to, which pertains to casting towards a supertype. This is further elaborated below.
///
/// # Rule Selection Criteria:
///
/// ## Forbidding Casts:
///
///  - Ambiguity: Prevents data from being loaded without errors but deviates from user expectations, which can be detrimental. Users might realize they have loaded incorrect data too late, potentially after the original data has been deleted.
///  - Uselessness: Prohibits casts that result in information loss and offer no benefit, often indicative of user error. Restricting such casts may guide users towards better practices.
///
/// ## Permitting Casts:
///
///  - Specificity: Encourages loading into a more specific type, as it typically requires less storage or provides more information. which is valuable in ETL processes.
///     -  Often accompanied by safety measures: Requires a specific format, and any mismatch will readily trigger an error, minimizing significant issues in production.
///  - Convenience: For instance, Python users might use the Python int type for convenience, which corresponds to int64 in Parquet. Thus, casting from int64 to smaller integers is allowed.
///  - Compatibility: Initially, the rules are based on the intersection of arrow_cast::can_cast_to() and all pairs that are operational from running run_cast, both of which are quite permissive.
///     - but maybe not a big issue, since user can start with infer_schema.
pub fn load_can_auto_cast_to(from_type: &DataType, to_type: &DataType) -> bool {
    use DataType::*;
    use NumberDataType::*;
    // note this does not cover diff Number(_) | Decimal(_)
    if from_type == to_type {
        return true;
    }
    // we mainly care about which types can/cannot cast to to_type.
    // the match branches is grouped in a way to make it easier to read this info.
    match (from_type, to_type) {
        (_, Null | EmptyArray | EmptyMap | Generic(_)) => unreachable!(),

        // ====  remove null first, all trivial
        (Null, Nullable(_)) => true,
        (Nullable(box from_ty), Nullable(box to_ty))
        | (from_ty, Nullable(box to_ty))
        | (Nullable(box from_ty), to_ty) => load_can_auto_cast_to(from_ty, to_ty),

        // ==== dive into nested types, must from the same out type, all trivial
        (Map(box from_ty), Map(box to_ty)) => match (from_ty, to_ty) {
            (Tuple(_), Tuple(_)) => load_can_auto_cast_to(from_ty, to_ty),
            (_, _) => unreachable!(),
        },
        (EmptyMap, Map(_)) => true,
        (_, Map(_)) | (Map(_), _) => false,

        (Tuple(from_tys), Tuple(to_tys)) => {
            from_tys.len() == to_tys.len()
                && from_tys
                    .iter()
                    .zip(to_tys)
                    .all(|(from_ty, to_ty)| load_can_auto_cast_to(from_ty, to_ty))
        }
        (_, Tuple(_)) | (Tuple(_), _) => false,

        (Array(box from_ty), Array(box to_ty)) => load_can_auto_cast_to(from_ty, to_ty),
        (EmptyArray, Array(_)) => true,
        (_, Array(_)) | (Array(_), _) => false,

        // ==== handle primary types at last, so the _ bellow only need to consider themselves.
        //
        // [specificity] binary to string return error if not utf8
        (Binary, String) => true,
        // [useless] other types all have a string repr, but lost info
        (_, String) => false,

        // [ambiguity] binary to encode to HEX string, may not what user expected, and diff from Binary -> String
        (Binary, Variant) => false,
        // [useless] cast_scalar_to_variant() accept all types (JSON map now accept only a few types as key),
        // but let`s not allow nested types at beginning (and need exam all inner types recursively if need it).
        // (Tuple(_) | Array | Map(_), Variant) => false,
        (_, Variant) => true,

        // [useless] be strict because it lost info without error
        // Numeric to boolean: 0 returns `false`, any other value returns `true`
        // notes: our cast func allow all Number(_) | Decimal(_); most db not allow float to bool; arrow not allow decimal to bool
        (Number(Int8 | UInt8), Boolean) => true,
        // [specificity] 'true', 'false' only, case insensitive.
        // TODO: make this an option like in CSV?
        (String, Boolean) => true,
        // bool or string
        (Variant, Boolean) => true,
        (_, Boolean) => false,

        (String | Number(_) | Decimal(_) | Boolean, Number(_) | Decimal(_)) => true,
        (_, Number(_) | Decimal(_)) => false,

        // [useless]: in SQL, cast a single int8 to Timestamp|Date is Ok, but cast such a column is not reasonable
        // (Int16, Date) => false: 1880-01-03 to 2159-12-30, disabled for now
        (Number(Int64 | Int32), Date) => true,
        // Int32 as seconds, overflow at year 2038
        // Int64 as seconds or milliseconds, depend on value
        // TODO: the whole columns should use the same interpreting? maybe the cast func should run with a context indicating it is loading data from column store
        (Number(Int64 | Int32), Timestamp) => true,
        // [specificity]
        (String | Variant | Timestamp | Date, Timestamp | Date) => true,
        (_, Timestamp | Date) => false,

        // [useless]
        (_, Binary) => false,

        // [specificity]
        (String, Bitmap) => true,
        // TODO: add Binary
        // [convenient] bitmap with only one bit set
        (Number(nt), Bitmap) => !nt.is_signed(),
        (_, Bitmap) => false,

        // [specificity]
        // String: all formats
        // Variant: GeoJson format
        // Binary: EWKB format
        (String | Binary | Variant, Geometry) => true,
        (_, Geometry) => false,

        // TODO:
        // (String | Binary | Variant, Geography) => true,
        (_, Geography) => false,
    }
}
