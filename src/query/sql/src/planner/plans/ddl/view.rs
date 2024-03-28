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

use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateViewPlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub column_names: Vec<String>,
    pub subquery: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterViewPlan {
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub column_names: Vec<String>,
    pub subquery: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropViewPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeViewPlan {
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub schema: DataSchemaRef,
}

impl DescribeViewPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
