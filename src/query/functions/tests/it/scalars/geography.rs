// Copyright 2022 Datafuse Labs.
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

use goldenfile::Mint;

use crate::scalars::run_ast;

#[test]
fn test_geography() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geography.txt").unwrap();
    test_st_makepoint(file);
}

fn test_st_makepoint(file: &mut impl Write) {
    run_ast(file, "st_makepoint(40.7127, -74.0059)", &[]);
}
