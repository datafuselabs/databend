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

use std::collections::HashMap;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use chrono_tz::Tz;
use databend_common_ast::ast::Hint;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Statement;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::SEARCH_MATCHED_COLUMN_ID;
use databend_common_expression::SEARCH_SCORE_COLUMN_ID;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use indexmap::IndexMap;
use log::warn;

use super::Finder;
use crate::binder::bind_query::ExpressionScanContext;
use crate::binder::util::illegal_ident_name;
use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::binder::CteInfo;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::CreateFileFormatPlan;
use crate::plans::CreateRolePlan;
use crate::plans::DescConnectionPlan;
use crate::plans::DescUserPlan;
use crate::plans::DropConnectionPlan;
use crate::plans::DropFileFormatPlan;
use crate::plans::DropRolePlan;
use crate::plans::DropStagePlan;
use crate::plans::DropUserPlan;
use crate::plans::MaterializedCte;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::RewriteKind;
use crate::plans::ShowConnectionsPlan;
use crate::plans::ShowFileFormatsPlan;
use crate::plans::ShowRolesPlan;
use crate::plans::UseDatabasePlan;
use crate::plans::Visitor;
use crate::BindContext;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarExpr;
use crate::TypeChecker;
use crate::Visibility;

/// Binder is responsible to transform AST of a query into a canonical logical SExpr.
///
/// During this phase, it will:
/// - Resolve columns and tables with Catalog
/// - Check semantic of query
/// - Validate expressions
/// - Build `Metadata`
#[derive(Clone)]
pub struct Binder {
    pub ctx: Arc<dyn TableContext>,
    pub dialect: Dialect,
    pub catalogs: Arc<CatalogManager>,
    pub name_resolution_ctx: NameResolutionContext,
    pub metadata: MetadataRef,
    // Save the bound context for materialized cte, the key is cte_idx
    pub m_cte_bound_ctx: HashMap<IndexType, BindContext>,
    pub m_cte_bound_s_expr: HashMap<IndexType, SExpr>,
    pub m_cte_materialized_indexes: HashMap<IndexType, IndexType>,
    /// Use `IndexMap` because need to keep the insertion order
    /// Then wrap materialized ctes to main plan.
    pub ctes_map: Box<IndexMap<String, CteInfo>>,
    /// The `ExpressionScanContext` is used to store the information of
    /// expression scan and hash join build cache.
    pub expression_scan_context: ExpressionScanContext,
    /// For the recursive cte, the cte table name occurs in the recursive cte definition and main query
    /// if meet recursive cte table name in cte definition, set `bind_recursive_cte` true and treat it as `CteScan`.
    pub bind_recursive_cte: bool,

    pub enable_result_cache: bool,
}

impl<'a> Binder {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        catalogs: Arc<CatalogManager>,
        name_resolution_ctx: NameResolutionContext,
        metadata: MetadataRef,
    ) -> Self {
        let dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
        let enable_result_cache = ctx
            .get_settings()
            .get_enable_query_result_cache()
            .unwrap_or_default();
        Binder {
            ctx,
            dialect,
            catalogs,
            name_resolution_ctx,
            metadata,
            m_cte_bound_ctx: Default::default(),
            m_cte_bound_s_expr: Default::default(),
            m_cte_materialized_indexes: Default::default(),
            ctes_map: Box::default(),
            expression_scan_context: ExpressionScanContext::new(),
            bind_recursive_cte: false,
            enable_result_cache,
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn bind(mut self, stmt: &Statement) -> Result<Plan> {
        let start = Instant::now();
        self.ctx.set_status_info("binding");
        let mut bind_context = BindContext::new();
        let plan = self.bind_statement(&mut bind_context, stmt).await?;
        self.bind_query_index(&mut bind_context, &plan).await?;
        self.ctx.set_status_info(&format!(
            "bind stmt to plan done, time used: {:?}",
            start.elapsed()
        ));
        Ok(plan)
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    pub(crate) async fn bind_statement(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &Statement,
    ) -> Result<Plan> {
        let plan = match stmt {
            Statement::Query(query) => {
                let (mut s_expr, bind_context) = self.bind_query(bind_context, query)?;

                // Wrap `LogicalMaterializedCte` to `s_expr`
                for (_, cte_info) in self.ctes_map.iter().rev() {
                    if !cte_info.materialized || cte_info.used_count == 0 {
                        continue;
                    }
                    let cte_s_expr = self.m_cte_bound_s_expr.get(&cte_info.cte_idx).unwrap();
                    let materialized_output_columns = cte_info.columns.clone();
                    s_expr = SExpr::create_binary(
                        Arc::new(RelOperator::MaterializedCte(MaterializedCte { cte_idx: cte_info.cte_idx, materialized_output_columns, materialized_indexes: self.m_cte_materialized_indexes.clone() })),
                        Arc::new(s_expr),
                        Arc::new(cte_s_expr.clone()),
                    );
                }

                // Remove unused cache columns and join conditions and construct ExpressionScan's child.
                (s_expr, _) = self.construct_expression_scan(&s_expr, self.metadata.clone())?;
                let formatted_ast = if self.ctx.get_settings().get_enable_query_result_cache()? {
                    Some(stmt.to_string())
                } else {
                    None
                };
                Plan::Query {
                    s_expr: Box::new(s_expr),
                    metadata: self.metadata.clone(),
                    bind_context: Box::new(bind_context),
                    rewrite_kind: None,
                    ignore_result: query.ignore_result,
                    formatted_ast,
                }
            }

            Statement::Explain { query, options, kind } => {
                self.bind_explain(bind_context, kind, options, query).await?
            }

            Statement::ExplainAnalyze {partial, graphical, query } => {
                let plan = self.bind_statement(bind_context, query).await?;
                Plan::ExplainAnalyze { partial: *partial, graphical: *graphical, plan: Box::new(plan) }
            }

            Statement::ShowFunctions { show_options } => {
                self.bind_show_functions(bind_context, show_options).await?
            }

            Statement::ShowUserFunctions { show_options } => {
                self.bind_show_user_functions(bind_context, show_options).await?
            }

            Statement::ShowTableFunctions { show_options } => {
                self.bind_show_table_functions(bind_context, show_options).await?
            }

            Statement::CopyIntoTable(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In Copy resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_copy_into_table(bind_context, stmt).await?
            }

            Statement::CopyIntoLocation(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In Copy resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_copy_into_location(bind_context, stmt).await?
            }

            Statement::ShowMetrics { show_options } => self.bind_show_metrics(bind_context, show_options).await?,
            Statement::ShowProcessList { show_options } => self.bind_show_process_list(bind_context, show_options).await?,
            Statement::ShowEngines { show_options } => self.bind_show_engines(bind_context, show_options).await?,
            Statement::ShowSettings { show_options } => self.bind_show_settings(bind_context, show_options).await?,
            Statement::ShowVariables { show_options } => self.bind_show_variables(bind_context, show_options).await?,
            Statement::ShowIndexes { show_options } => self.bind_show_indexes(bind_context, show_options).await?,
            Statement::ShowLocks(stmt) => self.bind_show_locks(bind_context, stmt).await?,
            // Catalogs
            Statement::ShowCatalogs(stmt) => self.bind_show_catalogs(bind_context, stmt).await?,
            Statement::ShowCreateCatalog(stmt) => self.bind_show_create_catalogs(stmt).await?,
            Statement::CreateCatalog(stmt) => self.bind_create_catalog(stmt).await?,
            Statement::DropCatalog(stmt) => self.bind_drop_catalog(stmt).await?,

            // Databases
            Statement::ShowDatabases(stmt) => self.bind_show_databases(bind_context, stmt).await?,
            Statement::ShowCreateDatabase(stmt) => self.bind_show_create_database(stmt).await?,
            Statement::CreateDatabase(stmt) => self.bind_create_database(stmt).await?,
            Statement::DropDatabase(stmt) => self.bind_drop_database(stmt).await?,
            Statement::UndropDatabase(stmt) => self.bind_undrop_database(stmt).await?,
            Statement::AlterDatabase(stmt) => self.bind_alter_database(stmt).await?,
            Statement::UseDatabase { database } => {
                let database = normalize_identifier(database, &self.name_resolution_ctx).name;
                Plan::UseDatabase(Box::new(UseDatabasePlan {
                    database,
                }))
            }
            // Columns
            Statement::ShowColumns(stmt) => self.bind_show_columns(bind_context, stmt).await?,
            // Tables
            Statement::ShowTables(stmt) => self.bind_show_tables(bind_context, stmt).await?,
            Statement::ShowCreateTable(stmt) => self.bind_show_create_table(stmt).await?,
            Statement::DescribeTable(stmt) => self.bind_describe_table(stmt).await?,
            Statement::ShowTablesStatus(stmt) => {
                self.bind_show_tables_status(bind_context, stmt).await?
            }
            Statement::ShowDropTables(stmt) => {
                self.bind_show_drop_tables(bind_context, stmt).await?
            }
            Statement::AttachTable(stmt) => self.bind_attach_table(stmt).await?,
            Statement::CreateTable(stmt) => self.bind_create_table(stmt).await?,
            Statement::DropTable(stmt) => self.bind_drop_table(stmt).await?,
            Statement::UndropTable(stmt) => self.bind_undrop_table(stmt).await?,
            Statement::AlterTable(stmt) => self.bind_alter_table(bind_context, stmt).await?,
            Statement::RenameTable(stmt) => self.bind_rename_table(stmt).await?,
            Statement::TruncateTable(stmt) => self.bind_truncate_table(stmt).await?,
            Statement::OptimizeTable(stmt) => self.bind_optimize_table(bind_context, stmt).await?,
            Statement::VacuumTable(stmt) => self.bind_vacuum_table(bind_context, stmt).await?,
            Statement::VacuumDropTable(stmt) => self.bind_vacuum_drop_table(bind_context, stmt).await?,
            Statement::VacuumTemporaryFiles(stmt) => self.bind_vacuum_temporary_files(bind_context, stmt).await?,
            Statement::AnalyzeTable(stmt) => self.bind_analyze_table(stmt).await?,
            Statement::ExistsTable(stmt) => self.bind_exists_table(stmt).await?,
            // Dictionaries
            Statement::CreateDictionary(stmt) => self.bind_create_dictionary(stmt).await?,
            Statement::DropDictionary(stmt) => self.bind_drop_dictionary(stmt).await?,
            Statement::ShowCreateDictionary(stmt) => self.bind_show_create_dictionary(stmt).await?,
            Statement::ShowDictionaries { show_options: _ } => todo!(),
            // Views
            Statement::CreateView(stmt) => self.bind_create_view(stmt).await?,
            Statement::AlterView(stmt) => self.bind_alter_view(stmt).await?,
            Statement::DropView(stmt) => self.bind_drop_view(stmt).await?,
            Statement::ShowViews(stmt) => self.bind_show_views(bind_context, stmt).await?,
            Statement::DescribeView(stmt) => self.bind_describe_view(stmt).await?,

            // Indexes
            Statement::CreateIndex(stmt) => self.bind_create_index(bind_context, stmt).await?,
            Statement::DropIndex(stmt) => self.bind_drop_index(stmt).await?,
            Statement::RefreshIndex(stmt) => self.bind_refresh_index(bind_context, stmt).await?,
            Statement::CreateInvertedIndex(stmt) => self.bind_create_inverted_index(bind_context, stmt).await?,
            Statement::DropInvertedIndex(stmt) => self.bind_drop_inverted_index(bind_context, stmt).await?,
            Statement::RefreshInvertedIndex(stmt) => self.bind_refresh_inverted_index(bind_context, stmt).await?,

            // Virtual Columns
            Statement::CreateVirtualColumn(stmt) => self.bind_create_virtual_column(stmt).await?,
            Statement::AlterVirtualColumn(stmt) => self.bind_alter_virtual_column(stmt).await?,
            Statement::DropVirtualColumn(stmt) => self.bind_drop_virtual_column(stmt).await?,
            Statement::RefreshVirtualColumn(stmt) => self.bind_refresh_virtual_column(stmt).await?,
            Statement::ShowVirtualColumns(stmt) => self.bind_show_virtual_columns(bind_context, stmt).await?,

            // Users
            Statement::CreateUser(stmt) => self.bind_create_user(stmt).await?,
            Statement::DropUser { if_exists, user } => Plan::DropUser(Box::new(DropUserPlan {
                if_exists: *if_exists,
                user: user.clone().into(),
            })),
            Statement::ShowUsers => self.bind_rewrite_to_query(bind_context, "SELECT name, hostname, auth_type, is_configured, default_role, roles, disabled, network_policy, password_policy, must_change_password FROM system.users ORDER BY name", RewriteKind::ShowUsers).await?,
            Statement::AlterUser(stmt) => self.bind_alter_user(stmt).await?,
            Statement::DescribeUser { user } => Plan::DescUser(Box::new(DescUserPlan {
                user: user.clone().into(),
            })),

            // Roles
            Statement::ShowRoles => Plan::ShowRoles(Box::new(ShowRolesPlan {})),
            Statement::CreateRole {
                if_not_exists,
                role_name,
            } => {
                if illegal_ident_name(role_name) {
                    return Err(ErrorCode::IllegalRole(
                        format!("Illegal Role Name: Illegal role name [{}], not support username contain ' or \"", role_name),
                    ));
                }
                Plan::CreateRole(Box::new(CreateRolePlan {
                if_not_exists: *if_not_exists,
                role_name: role_name.to_string(),
            }))},
            Statement::DropRole {
                if_exists,
                role_name,
            } => Plan::DropRole(Box::new(DropRolePlan {
                if_exists: *if_exists,
                role_name: role_name.to_string(),
            })),

            // Stages
            Statement::ShowStages => self.bind_rewrite_to_query(bind_context, "SELECT name, stage_type, number_of_files, creator, created_on, comment FROM system.stages ORDER BY name", RewriteKind::ShowStages).await?,
            Statement::ListStage { location, pattern } => {
                let pattern = if let Some(pattern) = pattern {
                    format!(", pattern => '{pattern}'")
                } else {
                    "".to_string()
                };
                self.bind_rewrite_to_query(bind_context, format!("SELECT * FROM LIST_STAGE(location => '@{location}'{pattern})").as_str(), RewriteKind::ListStage).await?
            }
            Statement::DescribeStage { stage_name } => self.bind_rewrite_to_query(bind_context, format!("SELECT * FROM system.stages WHERE name = '{stage_name}'").as_str(), RewriteKind::DescribeStage).await?,
            Statement::CreateStage(stmt) => self.bind_create_stage(stmt).await?,
            Statement::DropStage {
                stage_name,
                if_exists,
            } => {
                // Check user stage.
                if stage_name == "~" {
                    return Err(ErrorCode::StagePermissionDenied(
                        "user stage is not allowed to be dropped",
                    ));
                }
                Plan::DropStage(Box::new(DropStagePlan {
                if_exists: *if_exists,
                name: stage_name.clone(),
            }))},
            Statement::RemoveStage { location, pattern } => {
                self.bind_remove_stage(location, pattern).await?
            }
            Statement::Insert(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In INSERT resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_insert(bind_context, stmt).await?
            }
            Statement::InsertMultiTable(stmt) => {
                self.bind_insert_multi_table(bind_context, stmt).await?
            }
            Statement::Replace(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In REPLACE resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_replace(bind_context, stmt).await?
            }
            Statement::MergeInto(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In Merge resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_merge_into(bind_context, stmt).await?
            }
            Statement::Delete(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In DELETE resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_delete(bind_context, stmt)
                    .await?
            }
            Statement::Update(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                        warn!("In UPDATE resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_update(bind_context, stmt).await?
            }

            // Permissions
            Statement::Grant(stmt) => self.bind_grant(stmt).await?,
            Statement::ShowGrants { principal, show_options } => self.bind_show_account_grants(bind_context, principal, show_options).await?,
            Statement::ShowObjectPrivileges(stmt) => self.bind_show_object_privileges(bind_context, stmt).await?,
            Statement::Revoke(stmt) => self.bind_revoke(stmt).await?,

            // File Formats
            Statement::CreateFileFormat { create_option, name, file_format_options } => {
                if StageFileFormatType::from_str(name).is_ok() {
                    return Err(ErrorCode::SyntaxException(format!(
                        "File format {name} is reserved"
                    )));
                }
                Plan::CreateFileFormat(Box::new(CreateFileFormatPlan {
                    create_option: create_option.clone().into(),
                    name: name.clone(),
                    file_format_params: FileFormatParams::try_from_reader( FileFormatOptionsReader::from_ast(file_format_options), false)?,
                }))
            }
            Statement::DropFileFormat {
                if_exists,
                name,
            } => Plan::DropFileFormat(Box::new(DropFileFormatPlan {
                if_exists: *if_exists,
                name: name.clone(),
            })),
            Statement::ShowFileFormats => Plan::ShowFileFormats(Box::new(ShowFileFormatsPlan {})),

            // Connections
            Statement::CreateConnection(stmt) => self.bind_create_connection(stmt).await?,
            Statement::DropConnection(stmt) => Plan::DropConnection(Box::new(DropConnectionPlan {
                if_exists: stmt.if_exists,
                name: stmt.name.to_string(),
            })),
            Statement::DescribeConnection(stmt) => Plan::DescConnection(Box::new(DescConnectionPlan {
                name: stmt.name.to_string(),
            })),
            Statement::ShowConnections(_) => Plan::ShowConnections(Box::new(ShowConnectionsPlan{})),

            // UDFs
            Statement::CreateUDF(stmt) => self.bind_create_udf(stmt).await?,
            Statement::AlterUDF(stmt) => self.bind_alter_udf(stmt).await?,
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => self.bind_drop_udf(*if_exists, udf_name).await?,
            Statement::Call(stmt) => self.bind_call(bind_context, stmt).await?,

            Statement::Presign(stmt) => self.bind_presign(bind_context, stmt).await?,

            Statement::SetStmt {set_type, identifiers, values } => {
                self.bind_set(bind_context, *set_type, identifiers, values)
                    .await?
            }

            Statement::UnSetStmt{unset_type, identifiers } => {
                self.bind_unset(bind_context, *unset_type, identifiers)
                    .await?
            }

            Statement::SetRole {
                is_default,
                role_name,
            } => {
                self.bind_set_role(bind_context, *is_default, role_name).await?
            }
            Statement::SetSecondaryRoles { option } => {
                self.bind_set_secondary_roles(bind_context, option).await?
            }

            Statement::KillStmt { kill_target, object_id } => {
                self.bind_kill_stmt(bind_context, kill_target, object_id.as_str())
                    .await?
            }

            Statement::CreateDatamaskPolicy(stmt) => {
                self.bind_create_data_mask_policy(stmt).await?
            }
            Statement::DropDatamaskPolicy(stmt) => {
                self.bind_drop_data_mask_policy(stmt).await?
            }
            Statement::DescDatamaskPolicy(stmt) => {
                self.bind_desc_data_mask_policy(stmt).await?
            }
            Statement::CreateNetworkPolicy(stmt) => {
                self.bind_create_network_policy(stmt).await?
            }
            Statement::AlterNetworkPolicy(stmt) => {
                self.bind_alter_network_policy(stmt).await?
            }
            Statement::DropNetworkPolicy(stmt) => {
                self.bind_drop_network_policy(stmt).await?
            }
            Statement::DescNetworkPolicy(stmt) => {
                self.bind_desc_network_policy(stmt).await?
            }
            Statement::ShowNetworkPolicies => {
                self.bind_show_network_policies().await?
            }
            Statement::CreatePasswordPolicy(stmt) => {
                self.bind_create_password_policy(stmt).await?
            }
            Statement::AlterPasswordPolicy(stmt) => {
                self.bind_alter_password_policy(stmt).await?
            }
            Statement::DropPasswordPolicy(stmt) => {
                self.bind_drop_password_policy(stmt).await?
            }
            Statement::DescPasswordPolicy(stmt) => {
                self.bind_desc_password_policy(stmt).await?
            }
            Statement::ShowPasswordPolicies{ show_options } => self.bind_show_password_policies(bind_context, show_options).await?,
            Statement::CreateTask(stmt) => {
                self.bind_create_task(stmt).await?
            }
            Statement::AlterTask(stmt) => {
                self.bind_alter_task(stmt).await?
            }
            Statement::DropTask(stmt) => {
                self.bind_drop_task(stmt).await?
            }
            Statement::DescribeTask(stmt) => {
                self.bind_describe_task(stmt).await?
            }
            Statement::ExecuteTask(stmt) => {
                self.bind_execute_task(stmt).await?
            }
            Statement::ShowTasks(stmt) => {
                self.bind_show_tasks(stmt).await?
            }

            // Streams
            Statement::CreateStream(stmt) => self.bind_create_stream(bind_context, stmt).await?,
            Statement::DropStream(stmt) => self.bind_drop_stream(stmt).await?,
            Statement::ShowStreams(stmt) => self.bind_show_streams(bind_context, stmt).await?,
            Statement::DescribeStream(stmt) => self.bind_describe_stream(bind_context, stmt).await?,

            // Dynamic Table
            Statement::CreateDynamicTable(stmt) => self.bind_create_dynamic_table(stmt).await?,

            Statement::CreatePipe(_) => {
                todo!()
            }
            Statement::DescribePipe(_) => {
                todo!()
            }
            Statement::AlterPipe(_) => {
                todo!()
            }
            Statement::DropPipe(_) => {
                todo!()
            }
            Statement::CreateNotification(stmt) => {
                self.bind_create_notification(stmt).await?
            }
            Statement::DropNotification(stmt) => {
                self.bind_drop_notification(stmt).await?
            }
            Statement::AlterNotification(stmt) => {
                self.bind_alter_notification(stmt).await?
            }
            Statement::DescribeNotification(stmt) => {
                self.bind_desc_notification(stmt).await?
            }
            Statement::CreateSequence(stmt) => {
                self.bind_create_sequence(stmt).await?
            }
            Statement::DropSequence(stmt) => {
                self.bind_drop_sequence(stmt).await?
            }
            Statement::Begin => Plan::Begin,
            Statement::Commit => Plan::Commit,
            Statement::Abort => Plan::Abort,
            Statement::ExecuteImmediate(stmt) => self.bind_execute_immediate(stmt).await?,
            Statement::SetPriority {priority, object_id} => {
                self.bind_set_priority(priority, object_id).await?
            },
            Statement::System(stmt) => self.bind_system(stmt).await?,
            Statement::CreateProcedure(stmt) => { if self.ctx.get_settings().get_enable_experimental_procedure()? {
                self.bind_create_procedure(stmt).await?
            } else {
                return Err(ErrorCode::SyntaxException("CREATE PROCEDURE, set enable_experimental_procedure=1"));
            }
            }
            Statement::DropProcedure(stmt) => { if self.ctx.get_settings().get_enable_experimental_procedure()? {
                self.bind_drop_procedure(stmt).await?
            } else {
                return Err(ErrorCode::SyntaxException("DROP PROCEDURE, set enable_experimental_procedure=1"));
            }  }
            Statement::ShowProcedures { show_options } => { if self.ctx.get_settings().get_enable_experimental_procedure()? {
                self.bind_show_procedures(bind_context, show_options).await?
            } else {
                return Err(ErrorCode::SyntaxException("SHOW PROCEDURES, set enable_experimental_procedure=1"));
            }  }
            Statement::DescProcedure(stmt) => { if self.ctx.get_settings().get_enable_experimental_procedure()? {
                self.bind_desc_procedure(stmt).await?
            } else {
                return Err(ErrorCode::SyntaxException("DESC PROCEDURE, set enable_experimental_procedure=1"));
            }  }
            Statement::CallProcedure(stmt) => {
                if self.ctx.get_settings().get_enable_experimental_procedure()? {
                    self.bind_call_procedure(bind_context, stmt).await?
                } else {
                    return Err(ErrorCode::SyntaxException("CALL PROCEDURE, set enable_experimental_procedure=1"));
                }
                }
        };

        match plan.kind() {
            QueryKind::Query { .. } | QueryKind::Explain { .. } => {}
            _ => {
                let meta_data_guard = self.metadata.read();
                let tables = meta_data_guard.tables();
                for t in tables {
                    if t.is_consume() {
                        return Err(ErrorCode::SyntaxException(
                            "WITH CONSUME only allowed in query",
                        ));
                    }
                }
            }
        }

        Ok(plan)
    }

    pub(crate) fn normalize_identifier(&self, ident: &Identifier) -> Identifier {
        normalize_identifier(ident, &self.name_resolution_ctx)
    }

    pub(crate) fn opt_hints_set_var(
        &mut self,
        bind_context: &mut BindContext,
        hints: &Hint,
    ) -> Result<()> {
        let mut type_checker = TypeChecker::try_create(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            false,
        )?;
        let mut hint_settings: HashMap<String, String> = HashMap::new();
        for hint in &hints.hints_list {
            let variable = &hint.name.name;
            let (scalar, _) = *type_checker.resolve(&hint.expr)?;

            let scalar = wrap_cast(&scalar, &DataType::String);
            let expr = scalar.as_expr()?;

            let (new_expr, _) =
                ConstantFolder::fold(&expr, &self.ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
            match new_expr {
                Expr::Constant { scalar, .. } => {
                    let value = scalar.into_string().unwrap();
                    if variable.to_lowercase().as_str() == "timezone" {
                        let tz = value.trim_matches(|c| c == '\'' || c == '\"');
                        tz.parse::<Tz>().map_err(|_| {
                            ErrorCode::InvalidTimezone(format!("Invalid Timezone: {:?}", value))
                        })?;
                    }
                    hint_settings.entry(variable.to_string()).or_insert(value);
                }
                _ => {
                    warn!("fold hints {:?} failed. value must be constant value", hint);
                }
            }
        }

        self.ctx.get_settings().set_batch_settings(&hint_settings)
    }

    // After the materialized cte was bound, add it to `m_cte_bound_ctx`
    pub fn set_m_cte_bound_ctx(&mut self, cte_idx: IndexType, bound_ctx: BindContext) {
        self.m_cte_bound_ctx.insert(cte_idx, bound_ctx);
    }

    pub fn set_m_cte_bound_s_expr(&mut self, cte_idx: IndexType, s_expr: SExpr) {
        self.m_cte_bound_s_expr.insert(cte_idx, s_expr);
    }

    pub fn set_bind_recursive_cte(&mut self, val: bool) {
        self.bind_recursive_cte = val;
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_rewrite_to_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &str,
        rewrite_kind_r: RewriteKind,
    ) -> Result<Plan> {
        let tokens = tokenize_sql(query)?;
        let (stmt, _) = parse_sql(&tokens, self.dialect)?;
        let mut plan = self.bind_statement(bind_context, &stmt).await?;

        if let Plan::Query { rewrite_kind, .. } = &mut plan {
            *rewrite_kind = Some(rewrite_kind_r)
        }
        Ok(plan)
    }

    /// Create a new ColumnBinding for derived column
    pub(crate) fn create_derived_column_binding(
        &mut self,
        column_name: String,
        data_type: DataType,
        scalar_expr: Option<ScalarExpr>,
    ) -> ColumnBinding {
        let index = self.metadata.write().add_derived_column(
            column_name.clone(),
            data_type.clone(),
            scalar_expr,
        );
        ColumnBindingBuilder::new(column_name, index, Box::new(data_type), Visibility::Visible)
            .build()
    }

    /// Normalize [[<catalog>].<database>].<object>
    /// object like table, view ...
    pub fn normalize_object_identifier_triple(
        &self,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        object: &Identifier,
    ) -> (String, String, String) {
        let catalog_name = catalog
            .as_ref()
            .map(|ident| self.normalize_identifier(ident).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database_name = database
            .as_ref()
            .map(|ident| self.normalize_identifier(ident).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let object_name = self.normalize_identifier(object).name;
        (catalog_name, database_name, object_name)
    }

    /// Normalize <identifier>
    pub fn normalize_object_identifier(&self, ident: &Identifier) -> String {
        normalize_identifier(ident, &self.name_resolution_ctx).name
    }

    pub(crate) fn check_allowed_scalar_expr_with_udf(&self, scalar: &ScalarExpr) -> Result<bool> {
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::SubqueryExpr(_)
                    | ScalarExpr::AsyncFunctionCall(_)
            )
        };
        let mut finder = Finder::new(&f);
        finder.visit(scalar)?;
        Ok(finder.scalars().is_empty())
    }

    pub(crate) fn check_allowed_scalar_expr(&self, scalar: &ScalarExpr) -> Result<bool> {
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::UDFCall(_)
                    | ScalarExpr::SubqueryExpr(_)
                    | ScalarExpr::AsyncFunctionCall(_)
            )
        };
        let mut finder = Finder::new(&f);
        finder.visit(scalar)?;
        Ok(finder.scalars().is_empty())
    }

    #[allow(dead_code)]
    pub(crate) fn check_sexpr_top(&self, s_expr: &SExpr) -> Result<bool> {
        let f = |scalar: &ScalarExpr| matches!(scalar, ScalarExpr::UDFCall(_));
        let mut finder = Finder::new(&f);
        Self::check_sexpr(s_expr, &mut finder)
    }

    #[recursive::recursive]
    pub(crate) fn check_sexpr<F>(s_expr: &'a SExpr, f: &'a mut Finder<'a, F>) -> Result<bool>
    where F: Fn(&ScalarExpr) -> bool {
        let result = match s_expr.plan.as_ref() {
            RelOperator::Scan(scan) => {
                f.reset_finder();
                if let Some(agg_info) = &scan.agg_index {
                    for predicate in &agg_info.predicates {
                        f.visit(predicate)?;
                    }
                    for selection in &agg_info.selection {
                        f.visit(&selection.scalar)?;
                    }
                }
                if let Some(predicates) = &scan.push_down_predicates {
                    for predicate in predicates {
                        f.visit(predicate)?;
                    }
                }
                f.scalars().is_empty()
            }
            RelOperator::Join(join) => {
                f.reset_finder();
                for condition in join.equi_conditions.iter() {
                    f.visit(&condition.left)?;
                    f.visit(&condition.right)?;
                }
                for condition in &join.non_equi_conditions {
                    f.visit(condition)?;
                }
                f.scalars().is_empty()
            }
            RelOperator::EvalScalar(eval) => {
                f.reset_finder();
                for item in &eval.items {
                    f.visit(&item.scalar)?;
                }
                f.scalars().is_empty()
            }
            RelOperator::Filter(filter) => {
                f.reset_finder();
                for predicate in &filter.predicates {
                    f.visit(predicate)?;
                }
                f.scalars().is_empty()
            }
            RelOperator::Aggregate(aggregate) => {
                f.reset_finder();
                for item in &aggregate.group_items {
                    f.visit(&item.scalar)?;
                }
                for item in &aggregate.aggregate_functions {
                    f.visit(&item.scalar)?;
                }
                f.scalars().is_empty()
            }
            RelOperator::Exchange(exchange) => {
                f.reset_finder();
                if let crate::plans::Exchange::Hash(hash) = exchange {
                    for scalar in hash {
                        f.visit(scalar)?;
                    }
                }
                f.scalars().is_empty()
            }
            RelOperator::Window(window) => {
                f.reset_finder();
                for scalar_item in &window.arguments {
                    f.visit(&scalar_item.scalar)?;
                }
                for scalar_item in &window.partition_by {
                    f.visit(&scalar_item.scalar)?;
                }
                for info in &window.order_by {
                    f.visit(&info.order_by_item.scalar)?;
                }
                f.scalars().is_empty()
            }
            RelOperator::Udf(_) => false,
            _ => true,
        };

        match result {
            true => {
                for child in &s_expr.children {
                    let mut finder = Finder::new(f.find_fn());
                    let flag = Self::check_sexpr(child.as_ref(), &mut finder)?;
                    if !flag {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            false => Ok(false),
        }
    }

    pub(crate) fn check_allowed_scalar_expr_with_subquery_for_copy_table(
        &self,
        scalar: &ScalarExpr,
    ) -> Result<bool> {
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AsyncFunctionCall(_)
            )
        };
        let mut finder = Finder::new(&f);
        finder.visit(scalar)?;
        Ok(finder.scalars().is_empty())
    }

    pub(crate) fn add_internal_column_into_expr(
        &mut self,
        bind_context: &mut BindContext,
        s_expr: SExpr,
    ) -> Result<SExpr> {
        if bind_context.bound_internal_columns.is_empty() {
            return Ok(s_expr);
        }
        // check inverted index license
        if !bind_context.inverted_index_map.is_empty() {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;
        }
        let bound_internal_columns = &bind_context.bound_internal_columns;
        let mut inverted_index_map = mem::take(&mut bind_context.inverted_index_map);
        let mut s_expr = s_expr;

        let mut has_score = false;
        let mut has_matched = false;
        for column_id in bound_internal_columns.keys() {
            if *column_id == SEARCH_SCORE_COLUMN_ID {
                has_score = true;
            } else if *column_id == SEARCH_MATCHED_COLUMN_ID {
                has_matched = true;
            }
        }
        if has_score && !has_matched {
            return Err(ErrorCode::SemanticError(
                "score function must run with match or query function".to_string(),
            ));
        }

        for (table_index, column_index) in bound_internal_columns.values() {
            let inverted_index = inverted_index_map.shift_remove(table_index).map(|mut i| {
                i.has_score = has_score;
                i
            });
            s_expr = SExpr::add_internal_column_index(
                &s_expr,
                *table_index,
                *column_index,
                &inverted_index,
            );
        }
        Ok(s_expr)
    }
}
