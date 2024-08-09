// Copyright 2023 Datafuse Labs.
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

use databend_common_exception::Result;
use databend_common_expression::serialize::read_decimal;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use ethnum::i256;
use num_bigint::BigInt;
use pretty_assertions::assert_eq;

#[test]
fn test_decimal_text_exact() -> Result<()> {
    let cases = vec!["", ".e", ".", ".a", "-", "+", "e1", "a", "1e1e1", "1.1.1"];

    for s in cases {
        let r = read_decimal::<i128>(s.as_bytes(), 10, 5, true);
        assert!(r.is_err(), "{s}: {r:?}");
    }
    Ok(())
}

#[test]
fn test_decimal_text() -> Result<()> {
    let cases = vec![
        ("0#", (0i128, 0, 0, 1)),
        ("+0#", (0i128, 0, 0, 2)),
        ("-0#", (0i128, 0, 0, 2)),
        ("00#", (0i128, 0, 0, 2)),
        ("0.0#", (0i128, 0, 0, 3)),
        ("00.0#", (0i128, 0, 0, 4)),
        ("00.00#", (0i128, 0, 0, 5)),
        (".0#", (0i128, 0, 0, 2)),
        (".00#", (0i128, 0, 0, 3)),
        ("0.#", (0i128, 0, 0, 2)),
        ("1#", (1i128, 1, 0, 1)),
        ("-1#", (-1i128, 1, 0, 2)),
        ("10#", (10i128, 2, 0, 2)),
        ("15.0e-1#", (15i128, 2, -1, 7)),
        ("15.0e1#", (15i128, 2, 1, 6)),
        ("15.0e-11#", (15i128, 2, -11, 8)),
        ("010.010#", (1001i128, 4, -2, 7)),
        ("0120.0210#", (120021i128, 6, -3, 9)),
        ("0120.0210e3#", (120021i128, 6, 0, 11)),
        ("0120.0210e-1#", (120021i128, 6, -4, 12)),
    ];

    for (s, l) in cases {
        let r = read_decimal::<i128>(s.as_bytes(), 10, 10, false);
        match r {
            Ok(r) => assert_eq!(l, r, "{s}: {l:?} != {r:?}"),
            Err(e) => panic!("{s}: {l:?} != {e:?}"),
        }
    }

    let cases = vec!["", "10000000000#"];

    for s in cases {
        let r = read_decimal::<i128>(s.as_bytes(), 10, 10, false);
        assert!(r.is_err(), "{s}: {r:?}");
    }

    Ok(())
}

#[test]
fn test_decimal_with_size_text() -> Result<()> {
    let cases = vec![
        ("0#", 0i128),
        ("+0#", 0i128),
        ("+1.1#", 1100i128),
        ("-0#", 0i128),
        ("00#", 0i128),
        ("0.0#", 0i128),
        ("00.0#", 0i128),
        ("00.00#", 0i128),
        (".0#", 0i128),
        (".00#", 0i128),
        ("0.#", 0i128),
        ("1#", 1000i128),
        ("-1#", -1000i128),
        ("10#", 10000i128),
        ("010.010#", 10010i128),
        ("0120.0210#", 120021i128),
        ("0120.0211#", 120021i128),
        (".0210#", 21i128),
        (".010#", 10i128),
        (".001#", 1i128),
        (".0210e3#", 21000i128),
        (".010e2#", 1000i128),
        ("0120.0210e-1#", 12002i128),
        ("15.0e-1#", 1500i128),
        ("15.0e1#", 150000i128),
        ("15.0e-11#", 0i128),
    ];

    let size = DecimalSize {
        precision: 6,
        scale: 3,
    };

    for (s, l) in cases {
        let r = read_decimal_with_size::<i128>(s.as_bytes(), size, false, true);
        match r {
            Ok(r) => assert_eq!((l, s.len() - 1), r, "{s}: {l:?} != {r:?}"),
            Err(e) => panic!("{s}: {l:?} != {e:?}"),
        }
    }

    let cases = vec!["", "10000000000#", "1e10"];

    for s in cases {
        let r = read_decimal_with_size::<i128>(s.as_bytes(), size, false, true);
        assert!(r.is_err(), "{s}: {r:?}");
    }

    Ok(())
}

#[test]
fn test_decimal_common_type() {
    let cases = vec![
        ((3, 3), DataType::Number(NumberDataType::UInt8), (6, 3)),
        ((3, 2), DataType::Number(NumberDataType::UInt8), (5, 2)),
        ((6, 3), DataType::Number(NumberDataType::UInt8), (6, 3)),
    ];

    for (a, b, c) in cases {
        let l = DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: a.0,
            scale: a.1,
        }));
        let expected = DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: c.0,
            scale: c.1,
        }));
        let r = common_super_type(l, b, &[]);
        assert_eq!(r, Some(expected));
    }
}

#[test]
fn test_float_to_128() {
    let cases = vec![
        (123.456, 123),
        (-123.456, -123),
        (0.0, 0),
        (0.123, 0),
        (-0.123, 0),
    ];

    for (a, b) in cases {
        let r = i128::from_float(a);
        assert_eq!(r, b);
    }
}

#[test]
fn test_from_bigint() {
    let cases = vec![
        ("0", 0i128),
        ("12345", 12345i128),
        ("-1", -1i128),
        ("-170141183460469231731687303715884105728", i128::MIN),
        ("170141183460469231731687303715884105727", i128::MAX),
    ];

    for (a, b) in cases {
        let r = BigInt::parse_bytes(a.as_bytes(), 10).unwrap();
        assert_eq!(i128::from_bigint(r), Some(b));
    }

    let cases = vec![
        ("0".to_string(), i256::ZERO),
        ("12345".to_string(), i256::from(12345)),
        ("-1".to_string(), i256::from(-1)),
        (
            "12".repeat(25),
            i256::from_str_radix(&"12".repeat(25), 10).unwrap(),
        ),
        (
            "1".repeat(26),
            i256::from_str_radix(&"1".repeat(26), 10).unwrap(),
        ),
        (i256::MIN.to_string(), i256::MIN),
        (i256::MAX.to_string(), i256::MAX),
    ];

    for (a, b) in cases {
        let r = BigInt::parse_bytes(a.as_bytes(), 10).unwrap();
        assert_eq!(i256::from_bigint(r), Some(b));
    }

    let cases = vec![
        ("1".repeat(78), None),
        ("12".repeat(78), None),
        ("234".repeat(78), None),
    ];

    for (a, b) in cases {
        let r = BigInt::parse_bytes(a.as_bytes(), 10).unwrap();
        assert_eq!(i256::from_bigint(r), b);
    }
}
