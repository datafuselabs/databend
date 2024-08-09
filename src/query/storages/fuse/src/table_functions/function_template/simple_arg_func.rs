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
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;

use crate::table_functions::SimpleTableFunc;

#[async_trait::async_trait]
pub trait SimpleArgFunc {
    type Args;
    fn schema() -> TableSchemaRef;

    fn is_local_func() -> bool {
        true
    }
    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        plan: &DataSourcePlan,
    ) -> Result<DataBlock>;
}

pub struct SimpleArgFuncTemplate<T>
where
    T: SimpleArgFunc,
    T::Args: for<'a> TryFrom<(&'a str, TableArgs)>,
    TableArgs: for<'a> From<&'a T::Args>,
{
    args: T::Args,
}

#[async_trait::async_trait]
impl<T> SimpleTableFunc for SimpleArgFuncTemplate<T>
where
    T: SimpleArgFunc + 'static,
    T::Args: for<'a> TryFrom<(&'a str, TableArgs), Error = ErrorCode> + Send + Sync,
    TableArgs: for<'a> From<&'a T::Args>,
    Self: Sized,
{
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }
    fn schema(&self) -> TableSchemaRef {
        T::schema()
    }

    fn is_local_func(&self) -> bool {
        T::is_local_func()
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        Ok(Some(T::apply(ctx, &self.args, plan).await?))
    }

    fn create(name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let args = (name, table_args).try_into()?;
        Ok(Self { args })
    }
}
