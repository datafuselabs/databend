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

use std::sync::Arc;

use databend_common_expression::Expr;
use parking_lot::RwLock;
use xorf::BinaryFuse16;

pub type MergeIntoSourceBuildSiphashkeys = (Vec<String>, Arc<RwLock<Vec<Vec<u64>>>>);

#[derive(Clone, Debug, Default)]
pub struct RuntimeFilterInfo {
    inlist: Vec<Expr<String>>,
    min_max: Vec<Expr<String>>,
    bloom: Vec<(String, BinaryFuse16)>,
    siphashes: MergeIntoSourceBuildSiphashkeys,
}

impl RuntimeFilterInfo {
    pub fn add_inlist(&mut self, expr: Expr<String>) {
        self.inlist.push(expr);
    }

    pub fn add_bloom(&mut self, bloom: (String, BinaryFuse16)) {
        self.bloom.push(bloom);
    }

    pub fn get_merge_into_source_build_siphashkeys(&self) -> MergeIntoSourceBuildSiphashkeys {
        self.siphashes.clone()
    }

    pub fn add_merge_into_source_build_siphashkeys(&mut self, digests: (String, Vec<u64>)) {
        self.siphashes.0.push(digests.0);
        let mut borrow_hash_keys = self.siphashes.1.write();
        borrow_hash_keys.push(digests.1)
    }

    pub fn add_min_max(&mut self, expr: Expr<String>) {
        self.min_max.push(expr);
    }

    pub fn get_inlist(&self) -> &Vec<Expr<String>> {
        &self.inlist
    }

    pub fn get_bloom(&self) -> &Vec<(String, BinaryFuse16)> {
        &self.bloom
    }

    pub fn get_min_max(&self) -> &Vec<Expr<String>> {
        &self.min_max
    }

    pub fn blooms(self) -> Vec<(String, BinaryFuse16)> {
        self.bloom
    }

    pub fn inlists(self) -> Vec<Expr<String>> {
        self.inlist
    }

    pub fn min_maxs(self) -> Vec<Expr<String>> {
        self.min_max
    }

    pub fn is_empty(&self) -> bool {
        self.inlist.is_empty()
            && self.bloom.is_empty()
            && self.min_max.is_empty()
            && self.siphashes.0.len() == 0
    }
}
