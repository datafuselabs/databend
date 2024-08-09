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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

use crate::sessions::TableContext;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;
use crate::Table;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct PlaceHolder;

#[typetag::serde(name = "placeholder")]
impl PartInfo for PlaceHolder {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<PlaceHolder>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        0
    }
}

#[async_trait::async_trait]
pub trait SimpleTableFunc: Send + Sync + 'static {
    fn get_engine_name(&self) -> String {
        "table_func_template".to_owned()
    }

    fn is_local_func(&self) -> bool {
        true
    }

    fn table_args(&self) -> Option<TableArgs>;

    fn schema(&self) -> TableSchemaRef;

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>>;

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized;
}

pub struct TableFunctionTemplate<T>
where T: SimpleTableFunc
{
    table_info: TableInfo,
    inner: Arc<T>,
}

impl<T> TableFunctionTemplate<T>
where T: SimpleTableFunc
{
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let func = T::create(table_func_name, table_args)?;
        let schema = func.schema();
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema,
                engine: func.get_engine_name(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(TableFunctionTemplate {
            table_info,
            inner: Arc::new(func),
        }))
    }
}

#[async_trait::async_trait]
impl<T: SimpleTableFunc> Table for TableFunctionTemplate<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn is_local(&self) -> bool {
        self.inner.is_local_func()
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        match self.inner.is_local_func() {
            true => Ok((
                PartStatistics::default(),
                Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                    PlaceHolder,
                ))]),
            )),
            false => Ok((
                PartStatistics::default(),
                Partitions::create(PartitionsShuffleKind::Broadcast, vec![Arc::new(Box::new(
                    PlaceHolder,
                ))]),
            )),
        }
    }

    fn table_args(&self) -> Option<TableArgs> {
        self.inner.table_args()
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                SimpleFunctionSource::create(ctx.clone(), output, self.inner.clone(), plan.clone())
            },
            1,
        )?;

        Ok(())
    }
}

impl<T> TableFunction for TableFunctionTemplate<T>
where T: SimpleTableFunc
{
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct SimpleFunctionSource<T>
where T: SimpleTableFunc
{
    finish: bool,
    ctx: Arc<dyn TableContext>,
    func: Arc<T>,
    plan: DataSourcePlan,
}

impl<T> SimpleFunctionSource<T>
where T: SimpleTableFunc
{
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        func: Arc<T>,
        plan: DataSourcePlan,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, SimpleFunctionSource {
            func,
            ctx,
            finish: false,
            plan,
        })
    }
}

#[async_trait::async_trait]
impl<T> AsyncSource for SimpleFunctionSource<T>
where T: SimpleTableFunc
{
    const NAME: &'static str = "template_func";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }
        self.finish = true;

        self.func.apply(&self.ctx, &self.plan).await
    }
}
