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

use chrono::Utc;
use databend_common_ast::ast::CallProcedureStmt;
use databend_common_ast::ast::CreateProcedureStmt;
use databend_common_ast::ast::DescProcedureStmt;
use databend_common_ast::ast::DropProcedureStmt;
use databend_common_ast::ast::ExecuteImmediateStmt;
use databend_common_ast::ast::ProcedureLanguage;
use databend_common_ast::ast::ProcedureType;
use databend_common_ast::ast::ShowOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_users::UserApiProvider;

use crate::binder::show::get_show_options;
use crate::plans::CallProcedurePlan;
use crate::plans::CreateProcedurePlan;
use crate::plans::DropProcedurePlan;
use crate::plans::ExecuteImmediatePlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::plans::SubqueryType;
use crate::resolve_type_name;
use crate::BindContext;
use crate::Binder;
use crate::ScalarExpr;
use crate::TypeChecker;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_execute_immediate(
        &mut self,
        stmt: &ExecuteImmediateStmt,
    ) -> Result<Plan> {
        let ExecuteImmediateStmt { script } = stmt;
        Ok(Plan::ExecuteImmediate(Box::new(ExecuteImmediatePlan {
            script: script.clone(),
        })))
    }

    pub async fn bind_create_procedure(&mut self, stmt: &CreateProcedureStmt) -> Result<Plan> {
        let CreateProcedureStmt {
            create_option,
            name,
            language,
            args,
            return_type,
            comment,
            script,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        // TODO:
        // 1. need parser name: ProcedureNameIdent = name + args
        // 2. need check script's return type and stmt.return_type

        let meta = self.procedure_meta(return_type, script, comment, language, args)?;
        Ok(Plan::CreateProcedure(Box::new(CreateProcedurePlan {
            create_option: create_option.clone().into(),
            tenant: tenant.to_owned(),
            name: ProcedureNameIdent::new(&tenant, ProcedureIdentity::from(name.clone())),
            meta,
        })))
    }

    pub async fn bind_drop_procedure(&mut self, stmt: &DropProcedureStmt) -> Result<Plan> {
        let DropProcedureStmt { name, if_exists } = stmt;

        let tenant = self.ctx.get_tenant();
        Ok(Plan::DropProcedure(Box::new(DropProcedurePlan {
            if_exists: *if_exists,
            tenant: tenant.to_owned(),
            name: ProcedureNameIdent::new(tenant, ProcedureIdentity::from(name.clone())),
        })))
    }

    pub async fn bind_desc_procedure(&mut self, _stmt: &DescProcedureStmt) -> Result<Plan> {
        todo!()
    }

    pub async fn bind_show_procedures(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT name, procedure_id, arguments, comment, description, created_on FROM system.procedures {} ORDER BY name {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowProcedures)
            .await
    }

    pub async fn bind_call_procedure(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CallProcedureStmt,
    ) -> Result<Plan> {
        let CallProcedureStmt {
            name,
            args: arguments,
        } = stmt;
        let tenant = self.ctx.get_tenant();
        let mut type_checker = TypeChecker::try_create(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            true,
        )?;
        let mut arg_types = vec![];
        for argument in arguments {
            let box (arg, mut arg_type) = type_checker.resolve(argument)?;
            if let ScalarExpr::SubqueryExpr(subquery) = &arg {
                if subquery.typ == SubqueryType::Scalar && !arg.data_type()?.is_nullable() {
                    arg_type = arg_type.wrap_nullable();
                }
            }
            arg_types.push(arg_type.to_string());
        }
        let procedure_ident = ProcedureIdentity::new(name, arg_types.join(","));
        let req = GetProcedureReq {
            inner: ProcedureNameIdent::new(tenant.clone(), procedure_ident.clone()),
        };

        let procedure = UserApiProvider::instance()
            .procedure_api(&tenant)
            .get_procedure(&req)
            .await?;
        if let Some(procedure) = procedure {
            if arg_types.is_empty() {
                Ok(Plan::ExecuteImmediate(Box::new(ExecuteImmediatePlan {
                    script: procedure.procedure_meta.script,
                })))
            } else {
                Ok(Plan::CallProcedure(Box::new(CallProcedurePlan {
                    script: procedure.procedure_meta.script,
                    arg_names: procedure.procedure_meta.arg_names,
                    args: arguments.clone(),
                })))
            }
        } else {
            Err(ErrorCode::UnknownProcedure(format!(
                "Unknown procedure {}",
                procedure_ident
            )))
        }
    }

    fn procedure_meta(
        &self,
        return_type: &[ProcedureType],
        script: &str,
        comment: &Option<String>,
        language: &ProcedureLanguage,
        args: &Option<Vec<ProcedureType>>,
    ) -> Result<ProcedureMeta> {
        let mut arg_names = vec![];
        if let Some(args) = args {
            for arg in args {
                if let Some(name) = &arg.name {
                    arg_names.push(name.to_string());
                }
            }
        }
        let mut return_types = Vec::with_capacity(return_type.len());
        for arg_type in return_type {
            return_types.push(DataType::from(&resolve_type_name(
                &arg_type.data_type,
                true,
            )?));
        }

        Ok(ProcedureMeta {
            return_types,
            arg_names,
            created_on: Utc::now(),
            updated_on: Utc::now(),
            script: script.to_string(),
            comment: if let Some(comment) = comment {
                comment.to_owned()
            } else {
                "".to_string()
            },
            procedure_language: match language {
                ProcedureLanguage::SQL => "SQL".to_string(),
            },
        })
    }
}
