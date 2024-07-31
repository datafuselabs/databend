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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::DescDatamaskPolicyPlan;
use databend_common_users::UserApiProvider;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use log::warn;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DescDataMaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescDatamaskPolicyPlan,
}

impl DescDataMaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescDatamaskPolicyPlan) -> Result<Self> {
        Ok(DescDataMaskInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescDataMaskInterpreter {
    fn name(&self) -> &str {
        "DescDataMaskInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)?;
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();
        let policy = handler
            .get_data_mask(meta_api, &self.ctx.get_tenant(), self.plan.name.clone())
            .await;

        let policy = match policy {
            Ok(policy) => policy,
            Err(err) => {
                warn!("DescDataMaskInterpreter err: {}", err);
                if err.code() != ErrorCode::UNKNOWN_DATAMASK {
                    return Err(err);
                }
                return Ok(PipelineBuildResult::create());
            }
        };

        let name: Vec<String> = vec![self.plan.name.clone()];
        let create_on: Vec<String> = vec![policy.create_on.to_string().clone()];
        let args = format!(
            "({})",
            policy
                .args
                .iter()
                .map(|(arg_name, arg_type)| format!("{} {}", arg_name, arg_type))
                .rev()
                .collect::<Vec<_>>()
                .join(",")
        );
        let signature: Vec<String> = vec![args.clone()];
        let return_type = vec![policy.return_type.clone()];
        let body = vec![policy.body.clone()];
        let comment = vec![match policy.comment {
            Some(comment) => comment.clone(),
            None => "".to_string().clone(),
        }];

        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(name),
            StringType::from_data(create_on),
            StringType::from_data(signature),
            StringType::from_data(return_type),
            StringType::from_data(body),
            StringType::from_data(comment),
        ])];
        PipelineBuildResult::from_blocks(blocks)
    }
}
