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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_cache_manager::CacheManager;

use crate::table_functions::string_literal;
use crate::table_functions::string_value;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::table_functions::TableArgs;

#[derive(Clone)]
pub struct SetCapacity {
    cache_name: String,
    capacity: u64,
}

impl From<&SetCapacity> for TableArgs {
    fn from(value: &SetCapacity) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(&value.cache_name),
            string_literal(&value.capacity.to_string()),
        ])
    }
}

impl TryFrom<(&str, TableArgs)> for SetCapacity {
    type Error = ErrorCode;

    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let args = table_args.expect_all_positioned(func_name, Some(2))?;
        let cache_name = string_value(&args[0])?;
        let capacity = string_value(&args[1])?.parse::<u64>()?;
        Ok(Self {
            cache_name,
            capacity,
        })
    }
}

pub struct SetCache;
#[async_trait::async_trait]
impl SimpleArgFunc for SetCache {
    type Args = SetCapacity;

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("result", TableDataType::String),
        ])
    }

    fn is_local_func() -> bool {
        // cache operation needs to be broadcast to all nodes
        false
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        _plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let cache_mgr = CacheManager::instance();
        cache_mgr.set_cache_capacity(&args.cache_name, args.capacity)?;

        let node = vec![ctx.get_cluster().local_id.clone()];
        let res = vec!["Ok".to_owned()];

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(node),
            StringType::from_data(res),
        ]))
    }
}

pub type SetCacheCapacityFunc = SimpleArgFuncTemplate<SetCache>;
