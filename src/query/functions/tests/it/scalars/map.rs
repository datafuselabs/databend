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

use std::io::Write;

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_map() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("map.txt").unwrap();

    test_create(file);
    test_get(file);
    test_map_keys(file);
    test_map_values(file);
    test_map_size(file);
    test_map_cat(file);
    test_map_contains_key(file);
    test_map_pick(file);
}

fn test_map_cat(file: &mut impl Write) {
    // Empty Inputs:: tests behavior with empty input maps
    run_ast(file, "map_cat({}, {})", &[]);
    run_ast(file, "map_cat({}, {'k1': 'v1'})", &[]);
    run_ast(file, "map_cat({'k1': 'v1'}, {})", &[]);

    // Basic Functionality:: evaluates core functionality
    let columns = [
        ("a_col", StringType::from_data(vec!["a_k1", "a_k2", "a_k3"])),
        ("b_col", StringType::from_data(vec!["b_k1", "b_k2", "b_k3"])),
        ("c_col", StringType::from_data(vec!["c_k1", "c_k2", "c_k3"])),
        ("d_col", StringType::from_data(vec!["aaa1", "aaa2", "aaa3"])),
        ("e_col", StringType::from_data(vec!["bbb1", "bbb2", "bbb3"])),
        ("f_col", StringType::from_data(vec!["ccc1", "ccc2", "ccc3"])),
    ];

    run_ast(
        file,
        "map_cat(map([a_col, b_col], [d_col, e_col]), map([c_col], [f_col]))",
        &columns,
    );

    run_ast(file, "map_cat({'k1':'v1','k2':'v2'}, {'k1':'abc'})", &[]);

    // Duplicate Keys:: assesses handling of duplicate keys
    let columns = [
        ("a_col", StringType::from_data(vec!["a_k1", "a_k2", "c_k3"])),
        ("b_col", StringType::from_data(vec!["b_k1", "c_k2", "b_k3"])),
        ("c_col", StringType::from_data(vec!["c_k1", "c_k2", "c_k3"])),
        ("d_col", StringType::from_data(vec!["aaa1", "aaa2", "aaa3"])),
        ("e_col", StringType::from_data(vec!["bbb1", "bbb2", "bbb3"])),
        ("f_col", StringType::from_data(vec!["ccc1", "ccc2", "ccc3"])),
    ];

    run_ast(
        file,
        "map_cat(map([a_col, b_col], [d_col, e_col]), map([c_col], [f_col]))",
        &columns,
    );

    // Map Size Variation:: tests behavior with different map sizes
    run_ast(file, "map_cat({'k1': 'v1', 'k2': 'v2'}, {'k3': 'v3'})", &[]);
    run_ast(file, "map_cat({'k1': 'v1'}, {'k2': 'v2', 'k3': 'v3'})", &[]);

    // Null Values:: validates behavior for null values
    run_ast(
        file,
        "map_cat({'k1': 'v1', 'k2': NULL}, {'k2': 'v2', 'k3': NULL})",
        &[],
    );

    // Nested Maps:: examines recursive merging capabilities
    run_ast(
        file,
        "map_cat({'k1': {'nk1': 'nv1'}, 'k2': {'nk2': 'nv2'}}, {'k2': {'nk3': 'nv3'}, 'k3': {'nk4': 'nv4'}})",
        &[],
    );

    run_ast(
        file,
        "map_cat({'k1': {'nk1': 'nv1'}, 'k2': {'nk2': 'nv2'}}, {'k1': {'nk1': 'new_nv1'}, 'k2': {'nk3': 'nv3'}})",
        &[],
    );
}

fn test_create(file: &mut impl Write) {
    run_ast(file, "map([], [])", &[]);
    run_ast(file, "map([1,2], ['a','b'])", &[]);
    run_ast(file, "map(['k1','k2','k3'], ['v1','v2','v3'])", &[]);

    run_ast(file, "map(1, 'a')", &[]);
    run_ast(file, "map(['k1','k2'], ['v1','v2','v3'])", &[]);
    run_ast(file, "map(['k1','k1'], ['v1','v2'])", &[]);

    let columns = [
        ("a_col", Int8Type::from_data(vec![1i8, 2, 3])),
        ("b_col", Int8Type::from_data(vec![4i8, 5, 6])),
        ("c_col", Int8Type::from_data(vec![7i8, 8, 9])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["a", "b", "c"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["d", "e", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["f", "", "g"], vec![true, false, true]),
        ),
    ];
    run_ast(
        file,
        "map([a_col, b_col, c_col], [d_col, e_col, f_col])",
        &columns,
    );
    run_ast(file, "map(['k1', 'k2'], [a_col, b_col])", &columns);
}

fn test_get(file: &mut impl Write) {
    run_ast(file, "map([],[])[1]", &[]);
    run_ast(file, "map([1,2],['a','b'])[1]", &[]);
    run_ast(file, "map([1,2],['a','b'])[10]", &[]);
    run_ast(file, "map(['a','b'],[1,2])['a']", &[]);
    run_ast(file, "map(['a','b'],[1,2])['x']", &[]);

    run_ast(file, "{}['k']", &[]);
    run_ast(file, "{1:NULL}[1]", &[]);
    run_ast(file, "{'k1':'v1','k2':'v2'}['k1']", &[]);
    run_ast(file, "{'k1':'v1','k2':'v2'}['k3']", &[]);

    run_ast(file, "map([k1,k2],[v1,v2])[1]", &[
        ("k1", Int16Type::from_data(vec![1i16, 2])),
        ("k2", Int16Type::from_data(vec![3i16, 4])),
        ("v1", StringType::from_data(vec!["v1", "v2"])),
        ("v2", StringType::from_data(vec!["v3", "v4"])),
    ]);
}

fn test_map_keys(file: &mut impl Write) {
    run_ast(file, "map_keys({})", &[]);
    run_ast(file, "map_keys({'a':1,'b':2,'c':3})", &[]);
    run_ast(file, "map_keys({1:'a',2:'b',3:'c'})", &[]);
    run_ast(file, "map_keys({'a':NULL,'b':2,'c':NULL})", &[]);

    let columns = [
        ("a_col", StringType::from_data(vec!["a", "b", "c"])),
        ("b_col", StringType::from_data(vec!["d", "e", "f"])),
        ("c_col", StringType::from_data(vec!["x", "y", "z"])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["v1", "v2", "v3"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["v4", "v5", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["v6", "", "v7"], vec![true, false, true]),
        ),
    ];
    run_ast(
        file,
        "map_keys(map([a_col, b_col, c_col], [d_col, e_col, f_col]))",
        &columns,
    );
}

fn test_map_contains_key(file: &mut impl Write) {
    run_ast(file, "map_contains_key({'a':1,'b':2,'c':3}, 'a')", &[]);
    run_ast(file, "map_contains_key({}, 'a')", &[]);
    run_ast(file, "map_contains_key({'a':1,'b':2,'c':3}, 'd')", &[]);
    run_ast(file, "map_contains_key({'a':NULL,'b':2,'c':NULL}, 'a')", &[
    ]);

    // Nested Maps:: examines recursive key checking capabilities
    let columns = [
        ("a_col", StringType::from_data(vec!["a", "b", "c"])),
        ("b_col", StringType::from_data(vec!["d", "e", "f"])),
        ("c_col", StringType::from_data(vec!["x", "y", "z"])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["v1", "v2", "v3"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["v4", "v5", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["v6", "", "v7"], vec![true, false, true]),
        ),
    ];
    run_ast(
        file,
        "map_contains_key(map([a_col, b_col, c_col], [d_col, e_col, f_col]), 'a')",
        &columns,
    );
    run_ast(
        file,
        "map_contains_key(map([a_col, b_col, c_col], [d_col, e_col, f_col]), 'd')",
        &columns,
    );
}

fn test_map_values(file: &mut impl Write) {
    run_ast(file, "map_values({})", &[]);
    run_ast(file, "map_values({'a':1,'b':2,'c':3})", &[]);
    run_ast(file, "map_values({1:'a',2:'b',3:'c'})", &[]);
    run_ast(file, "map_values({'a':NULL,'b':2,'c':NULL})", &[]);

    let columns = [
        ("a_col", StringType::from_data(vec!["a", "b", "c"])),
        ("b_col", StringType::from_data(vec!["d", "e", "f"])),
        ("c_col", StringType::from_data(vec!["x", "y", "z"])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["v1", "v2", "v3"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["v4", "v5", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["v6", "", "v7"], vec![true, false, true]),
        ),
    ];
    run_ast(
        file,
        "map_values(map([a_col, b_col, c_col], [d_col, e_col, f_col]))",
        &columns,
    );
}

fn test_map_size(file: &mut impl Write) {
    run_ast(file, "map_size({})", &[]);
    run_ast(file, "map_size({'a':1,'b':2,'c':3})", &[]);
    run_ast(file, "map_size({'a':NULL,'b':2,'c':NULL})", &[]);

    let columns = [
        ("a_col", StringType::from_data(vec!["a", "b", "c"])),
        ("b_col", StringType::from_data(vec!["d", "e", "f"])),
        ("c_col", StringType::from_data(vec!["x", "y", "z"])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["v1", "v2", "v3"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["v4", "v5", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["v6", "", "v7"], vec![true, false, true]),
        ),
    ];
    run_ast(
        file,
        "map_size(map([a_col, b_col, c_col], [d_col, e_col, f_col]))",
        &columns,
    );
}

fn test_map_pick(file: &mut impl Write) {
    run_ast(file, "map_pick({'a':1,'b':2,'c':3}, 'a', 'b')", &[]);
    run_ast(file, "map_pick({'a':1,'b':2,'c':3}, ['a', 'b'])", &[]);

    let columns = [
        ("a_col", StringType::from_data(vec!["a", "b", "c"])),
        ("b_col", StringType::from_data(vec!["d", "e", "f"])),
        ("c_col", StringType::from_data(vec!["x", "y", "z"])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["v1", "v2", "v3"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["v4", "v5", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["v6", "", "v7"], vec![true, false, true]),
        ),
    ];
    run_ast(
        file,
        "map_pick(map([a_col, b_col, c_col], [d_col, e_col, f_col]), 'a', 'b')",
        &columns,
    );
}
