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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::binder::ExplainConfig;
use crate::optimizer::SExpr;
use crate::plans::copy_into_location::CopyIntoLocationPlan;
use crate::plans::AddTableColumnPlan;
use crate::plans::AlterNetworkPolicyPlan;
use crate::plans::AlterNotificationPlan;
use crate::plans::AlterPasswordPolicyPlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AlterTaskPlan;
use crate::plans::AlterUDFPlan;
use crate::plans::AlterUserPlan;
use crate::plans::AlterViewPlan;
use crate::plans::AlterVirtualColumnPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CopyIntoTableMode;
use crate::plans::CopyIntoTablePlan;
use crate::plans::CreateCatalogPlan;
use crate::plans::CreateConnectionPlan;
use crate::plans::CreateDatabasePlan;
use crate::plans::CreateDatamaskPolicyPlan;
use crate::plans::CreateDynamicTablePlan;
use crate::plans::CreateFileFormatPlan;
use crate::plans::CreateIndexPlan;
use crate::plans::CreateNetworkPolicyPlan;
use crate::plans::CreateNotificationPlan;
use crate::plans::CreatePasswordPolicyPlan;
use crate::plans::CreateRolePlan;
use crate::plans::CreateSequencePlan;
use crate::plans::CreateStagePlan;
use crate::plans::CreateStreamPlan;
use crate::plans::CreateTableIndexPlan;
use crate::plans::CreateTablePlan;
use crate::plans::CreateTaskPlan;
use crate::plans::CreateUDFPlan;
use crate::plans::CreateUserPlan;
use crate::plans::CreateViewPlan;
use crate::plans::CreateVirtualColumnPlan;
use crate::plans::DeletePlan;
use crate::plans::DescConnectionPlan;
use crate::plans::DescDatamaskPolicyPlan;
use crate::plans::DescNetworkPolicyPlan;
use crate::plans::DescNotificationPlan;
use crate::plans::DescPasswordPolicyPlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DescribeTaskPlan;
use crate::plans::DescribeViewPlan;
use crate::plans::DropCatalogPlan;
use crate::plans::DropConnectionPlan;
use crate::plans::DropDatabasePlan;
use crate::plans::DropDatamaskPolicyPlan;
use crate::plans::DropFileFormatPlan;
use crate::plans::DropIndexPlan;
use crate::plans::DropNetworkPolicyPlan;
use crate::plans::DropNotificationPlan;
use crate::plans::DropPasswordPolicyPlan;
use crate::plans::DropRolePlan;
use crate::plans::DropSequencePlan;
use crate::plans::DropStagePlan;
use crate::plans::DropStreamPlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTableIndexPlan;
use crate::plans::DropTablePlan;
use crate::plans::DropTaskPlan;
use crate::plans::DropUDFPlan;
use crate::plans::DropUserPlan;
use crate::plans::DropViewPlan;
use crate::plans::DropVirtualColumnPlan;
use crate::plans::ExecuteImmediatePlan;
use crate::plans::ExecuteTaskPlan;
use crate::plans::ExistsTablePlan;
use crate::plans::GrantPrivilegePlan;
use crate::plans::GrantRolePlan;
use crate::plans::Insert;
use crate::plans::InsertMultiTable;
use crate::plans::KillPlan;
use crate::plans::MergeInto;
use crate::plans::ModifyTableColumnPlan;
use crate::plans::ModifyTableCommentPlan;
use crate::plans::OptimizeTablePlan;
use crate::plans::PresignPlan;
use crate::plans::ReclusterTablePlan;
use crate::plans::RefreshIndexPlan;
use crate::plans::RefreshTableIndexPlan;
use crate::plans::RefreshVirtualColumnPlan;
use crate::plans::RemoveStagePlan;
use crate::plans::RenameDatabasePlan;
use crate::plans::RenameTableColumnPlan;
use crate::plans::RenameTablePlan;
use crate::plans::Replace;
use crate::plans::RevertTablePlan;
use crate::plans::RevokePrivilegePlan;
use crate::plans::RevokeRolePlan;
use crate::plans::SetOptionsPlan;
use crate::plans::SetPriorityPlan;
use crate::plans::SetRolePlan;
use crate::plans::SetSecondaryRolesPlan;
use crate::plans::SettingPlan;
use crate::plans::ShowConnectionsPlan;
use crate::plans::ShowCreateCatalogPlan;
use crate::plans::ShowCreateDatabasePlan;
use crate::plans::ShowCreateTablePlan;
use crate::plans::ShowFileFormatsPlan;
use crate::plans::ShowGrantsPlan;
use crate::plans::ShowNetworkPoliciesPlan;
use crate::plans::ShowRolesPlan;
use crate::plans::ShowTasksPlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UnSettingPlan;
use crate::plans::UndropDatabasePlan;
use crate::plans::UndropTablePlan;
use crate::plans::UpdatePlan;
use crate::plans::UseDatabasePlan;
use crate::plans::VacuumDropTablePlan;
use crate::plans::VacuumTablePlan;
use crate::plans::VacuumTemporaryFilesPlan;
use crate::BindContext;
use crate::MetadataRef;

#[derive(Clone, Debug)]
pub enum Plan {
    // `SELECT` statement
    Query {
        s_expr: Box<SExpr>,
        metadata: MetadataRef,
        bind_context: Box<BindContext>,
        rewrite_kind: Option<RewriteKind>,
        // Use for generate query result cache key.
        formatted_ast: Option<String>,
        ignore_result: bool,
    },

    Explain {
        kind: ExplainKind,
        config: ExplainConfig,
        plan: Box<Plan>,
    },
    ExplainAst {
        formatted_string: String,
    },
    ExplainSyntax {
        formatted_sql: String,
    },
    ExplainAnalyze {
        plan: Box<Plan>,
    },

    // Call is rewrite into Query
    // Call(Box<CallPlan>),

    // Catalogs
    ShowCreateCatalog(Box<ShowCreateCatalogPlan>),
    CreateCatalog(Box<CreateCatalogPlan>),
    DropCatalog(Box<DropCatalogPlan>),

    // Databases
    ShowCreateDatabase(Box<ShowCreateDatabasePlan>),
    CreateDatabase(Box<CreateDatabasePlan>),
    DropDatabase(Box<DropDatabasePlan>),
    UndropDatabase(Box<UndropDatabasePlan>),
    RenameDatabase(Box<RenameDatabasePlan>),
    UseDatabase(Box<UseDatabasePlan>),

    // Tables
    ShowCreateTable(Box<ShowCreateTablePlan>),
    DescribeTable(Box<DescribeTablePlan>),
    CreateTable(Box<CreateTablePlan>),
    DropTable(Box<DropTablePlan>),
    UndropTable(Box<UndropTablePlan>),
    RenameTable(Box<RenameTablePlan>),
    ModifyTableComment(Box<ModifyTableCommentPlan>),
    RenameTableColumn(Box<RenameTableColumnPlan>),
    AddTableColumn(Box<AddTableColumnPlan>),
    DropTableColumn(Box<DropTableColumnPlan>),
    ModifyTableColumn(Box<ModifyTableColumnPlan>),
    AlterTableClusterKey(Box<AlterTableClusterKeyPlan>),
    DropTableClusterKey(Box<DropTableClusterKeyPlan>),
    ReclusterTable(Box<ReclusterTablePlan>),
    RevertTable(Box<RevertTablePlan>),
    TruncateTable(Box<TruncateTablePlan>),
    OptimizeTable(Box<OptimizeTablePlan>),
    VacuumTable(Box<VacuumTablePlan>),
    VacuumDropTable(Box<VacuumDropTablePlan>),
    VacuumTemporaryFiles(Box<VacuumTemporaryFilesPlan>),
    AnalyzeTable(Box<AnalyzeTablePlan>),
    ExistsTable(Box<ExistsTablePlan>),
    SetOptions(Box<SetOptionsPlan>),

    // Insert
    Insert(Box<Insert>),
    InsertMultiTable(Box<InsertMultiTable>),
    Replace(Box<Replace>),
    Delete(Box<DeletePlan>),
    Update(Box<UpdatePlan>),
    MergeInto(Box<MergeInto>),

    CopyIntoTable(Box<CopyIntoTablePlan>),
    CopyIntoLocation(CopyIntoLocationPlan),

    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),
    DropView(Box<DropViewPlan>),
    DescribeView(Box<DescribeViewPlan>),

    // Streams
    CreateStream(Box<CreateStreamPlan>),
    DropStream(Box<DropStreamPlan>),

    // Indexes
    CreateIndex(Box<CreateIndexPlan>),
    DropIndex(Box<DropIndexPlan>),
    RefreshIndex(Box<RefreshIndexPlan>),
    CreateTableIndex(Box<CreateTableIndexPlan>),
    DropTableIndex(Box<DropTableIndexPlan>),
    RefreshTableIndex(Box<RefreshTableIndexPlan>),

    // Virtual Columns
    CreateVirtualColumn(Box<CreateVirtualColumnPlan>),
    AlterVirtualColumn(Box<AlterVirtualColumnPlan>),
    DropVirtualColumn(Box<DropVirtualColumnPlan>),
    RefreshVirtualColumn(Box<RefreshVirtualColumnPlan>),

    // Account
    AlterUser(Box<AlterUserPlan>),
    CreateUser(Box<CreateUserPlan>),
    DropUser(Box<DropUserPlan>),

    // UDF
    CreateUDF(Box<CreateUDFPlan>),
    AlterUDF(Box<AlterUDFPlan>),
    DropUDF(Box<DropUDFPlan>),

    // Role
    ShowRoles(Box<ShowRolesPlan>),
    CreateRole(Box<CreateRolePlan>),
    DropRole(Box<DropRolePlan>),
    GrantRole(Box<GrantRolePlan>),
    GrantPriv(Box<GrantPrivilegePlan>),
    ShowGrants(Box<ShowGrantsPlan>),
    RevokePriv(Box<RevokePrivilegePlan>),
    RevokeRole(Box<RevokeRolePlan>),
    SetRole(Box<SetRolePlan>),
    SetSecondaryRoles(Box<SetSecondaryRolesPlan>),

    // FileFormat
    CreateFileFormat(Box<CreateFileFormatPlan>),
    DropFileFormat(Box<DropFileFormatPlan>),
    ShowFileFormats(Box<ShowFileFormatsPlan>),

    // Stages
    CreateStage(Box<CreateStagePlan>),
    DropStage(Box<DropStagePlan>),
    RemoveStage(Box<RemoveStagePlan>),

    // Connection
    CreateConnection(Box<CreateConnectionPlan>),
    DescConnection(Box<DescConnectionPlan>),
    DropConnection(Box<DropConnectionPlan>),
    ShowConnections(Box<ShowConnectionsPlan>),

    // Presign
    Presign(Box<PresignPlan>),

    // Set
    SetVariable(Box<SettingPlan>),
    UnSetVariable(Box<UnSettingPlan>),
    Kill(Box<KillPlan>),
    SetPriority(Box<SetPriorityPlan>),

    // Data mask
    CreateDatamaskPolicy(Box<CreateDatamaskPolicyPlan>),
    DropDatamaskPolicy(Box<DropDatamaskPolicyPlan>),
    DescDatamaskPolicy(Box<DescDatamaskPolicyPlan>),

    // Network policy
    CreateNetworkPolicy(Box<CreateNetworkPolicyPlan>),
    AlterNetworkPolicy(Box<AlterNetworkPolicyPlan>),
    DropNetworkPolicy(Box<DropNetworkPolicyPlan>),
    DescNetworkPolicy(Box<DescNetworkPolicyPlan>),
    ShowNetworkPolicies(Box<ShowNetworkPoliciesPlan>),

    // Password policy
    CreatePasswordPolicy(Box<CreatePasswordPolicyPlan>),
    AlterPasswordPolicy(Box<AlterPasswordPolicyPlan>),
    DropPasswordPolicy(Box<DropPasswordPolicyPlan>),
    DescPasswordPolicy(Box<DescPasswordPolicyPlan>),

    // Task
    CreateTask(Box<CreateTaskPlan>),
    AlterTask(Box<AlterTaskPlan>),
    DropTask(Box<DropTaskPlan>),
    DescribeTask(Box<DescribeTaskPlan>),
    ShowTasks(Box<ShowTasksPlan>),
    ExecuteTask(Box<ExecuteTaskPlan>),

    CreateDynamicTable(Box<CreateDynamicTablePlan>),

    // Txn
    Begin,
    Commit,
    Abort,

    // Notifications
    CreateNotification(Box<CreateNotificationPlan>),
    AlterNotification(Box<AlterNotificationPlan>),
    DropNotification(Box<DropNotificationPlan>),
    DescNotification(Box<DescNotificationPlan>),

    // Stored procedures
    ExecuteImmediate(Box<ExecuteImmediatePlan>),

    // sequence
    CreateSequence(Box<CreateSequencePlan>),
    DropSequence(Box<DropSequencePlan>),
}

#[derive(Clone, Debug)]
pub enum RewriteKind {
    ShowSettings,
    ShowMetrics,
    ShowProcessList,
    ShowEngines,
    ShowIndexes,

    ShowLocks,

    ShowCatalogs,
    ShowDatabases,
    ShowTables(String, String),
    ShowColumns(String, String, String),
    ShowTablesStatus,
    ShowVirtualColumns,

    ShowStreams(String),

    ShowFunctions,
    ShowUserFunctions,
    ShowTableFunctions,

    ShowUsers,
    ShowStages,
    DescribeStage,
    ListStage,
    ShowRoles,
    ShowPasswordPolicies,

    Call,
}

impl Plan {
    pub fn kind(&self) -> QueryKind {
        match self {
            Plan::Query { .. } => QueryKind::Query,
            Plan::CopyIntoTable(copy_plan) => match copy_plan.write_mode {
                CopyIntoTableMode::Insert { .. } => QueryKind::Insert,
                _ => QueryKind::CopyIntoTable,
            },
            Plan::Explain { .. }
            | Plan::ExplainAnalyze { .. }
            | Plan::ExplainAst { .. }
            | Plan::ExplainSyntax { .. } => QueryKind::Explain,
            Plan::Insert(_) => QueryKind::Insert,
            Plan::Replace(_)
            | Plan::Delete(_)
            | Plan::MergeInto(_)
            | Plan::OptimizeTable(_)
            | Plan::Update(_) => QueryKind::Update,
            _ => QueryKind::Other,
        }
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind())
    }
}

impl Plan {
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Plan::Query {
                s_expr: _,
                metadata: _,
                bind_context,
                ..
            } => bind_context.output_schema(),
            Plan::Explain { .. }
            | Plan::ExplainAst { .. }
            | Plan::ExplainSyntax { .. }
            | Plan::ExplainAnalyze { .. } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", DataType::String)])
            }
            Plan::ShowCreateCatalog(plan) => plan.schema(),
            Plan::ShowCreateDatabase(plan) => plan.schema(),
            Plan::ShowCreateTable(plan) => plan.schema(),
            Plan::DescribeTable(plan) => plan.schema(),
            Plan::VacuumTable(plan) => plan.schema(),
            Plan::VacuumDropTable(plan) => plan.schema(),
            Plan::VacuumTemporaryFiles(plan) => plan.schema(),
            Plan::ExistsTable(plan) => plan.schema(),
            Plan::DescribeView(plan) => plan.schema(),
            Plan::ShowRoles(plan) => plan.schema(),
            Plan::ShowGrants(plan) => plan.schema(),
            Plan::ShowFileFormats(plan) => plan.schema(),
            Plan::Replace(plan) => plan.schema(),
            Plan::Presign(plan) => plan.schema(),
            Plan::CreateDatamaskPolicy(plan) => plan.schema(),
            Plan::DropDatamaskPolicy(plan) => plan.schema(),
            Plan::DescDatamaskPolicy(plan) => plan.schema(),
            Plan::DescNetworkPolicy(plan) => plan.schema(),
            Plan::ShowNetworkPolicies(plan) => plan.schema(),
            Plan::DescPasswordPolicy(plan) => plan.schema(),
            Plan::CopyIntoTable(plan) => plan.schema(),
            Plan::CopyIntoLocation(plan) => plan.schema(),
            Plan::MergeInto(plan) => plan.schema(),
            Plan::CreateTask(plan) => plan.schema(),
            Plan::DescribeTask(plan) => plan.schema(),
            Plan::ShowTasks(plan) => plan.schema(),
            Plan::ExecuteTask(plan) => plan.schema(),
            Plan::DescNotification(plan) => plan.schema(),
            Plan::DescConnection(plan) => plan.schema(),
            Plan::ShowConnections(plan) => plan.schema(),
            Plan::ExecuteImmediate(plan) => plan.schema(),
            Plan::InsertMultiTable(plan) => plan.schema(),

            _ => Arc::new(DataSchema::empty()),
        }
    }

    pub fn has_result_set(&self) -> bool {
        !self.schema().fields().is_empty()
    }
}
