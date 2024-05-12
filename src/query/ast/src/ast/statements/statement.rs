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

use databend_common_io::escape_string_with_quote;
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::schema::CreateOption;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;

use super::merge_into::MergeIntoStmt;
use super::*;
use crate::ast::statements::connection::CreateConnectionStmt;
use crate::ast::statements::pipe::CreatePipeStmt;
use crate::ast::statements::task::CreateTaskStmt;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;

// SQL statement
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum Statement {
    Query(Box<Query>),
    Explain {
        kind: ExplainKind,
        options: Vec<ExplainOption>,
        query: Box<Statement>,
    },
    ExplainAnalyze {
        query: Box<Statement>,
    },

    CopyIntoTable(CopyIntoTableStmt),
    CopyIntoLocation(CopyIntoLocationStmt),

    Call(CallStmt),

    ShowSettings {
        show_options: Option<ShowOptions>,
    },
    ShowProcessList {
        show_options: Option<ShowOptions>,
    },
    ShowMetrics {
        show_options: Option<ShowOptions>,
    },
    ShowEngines {
        show_options: Option<ShowOptions>,
    },
    ShowFunctions {
        show_options: Option<ShowOptions>,
    },
    ShowUserFunctions {
        show_options: Option<ShowOptions>,
    },
    ShowTableFunctions {
        show_options: Option<ShowOptions>,
    },
    ShowIndexes {
        show_options: Option<ShowOptions>,
    },
    ShowLocks(ShowLocksStmt),

    KillStmt {
        kill_target: KillTarget,
        #[drive(skip)]
        object_id: String,
    },

    SetVariable {
        #[drive(skip)]
        is_global: bool,
        variable: Identifier,
        value: Box<Expr>,
    },

    UnSetVariable(UnSetStmt),

    SetRole {
        #[drive(skip)]
        is_default: bool,
        #[drive(skip)]
        role_name: String,
    },

    SetSecondaryRoles {
        option: SecondaryRolesOption,
    },

    Insert(InsertStmt),
    InsertMultiTable(InsertMultiTableStmt),
    Replace(ReplaceStmt),
    MergeInto(MergeIntoStmt),
    Delete(DeleteStmt),

    Update(UpdateStmt),

    // Catalogs
    ShowCatalogs(ShowCatalogsStmt),
    ShowCreateCatalog(ShowCreateCatalogStmt),
    CreateCatalog(CreateCatalogStmt),
    DropCatalog(DropCatalogStmt),

    // Databases
    ShowDatabases(ShowDatabasesStmt),
    ShowCreateDatabase(ShowCreateDatabaseStmt),
    CreateDatabase(CreateDatabaseStmt),
    DropDatabase(DropDatabaseStmt),
    UndropDatabase(UndropDatabaseStmt),
    AlterDatabase(AlterDatabaseStmt),
    UseDatabase {
        database: Identifier,
    },

    // Tables
    ShowTables(ShowTablesStmt),
    ShowCreateTable(ShowCreateTableStmt),
    DescribeTable(DescribeTableStmt),
    ShowTablesStatus(ShowTablesStatusStmt),
    ShowDropTables(ShowDropTablesStmt),
    AttachTable(AttachTableStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    UndropTable(UndropTableStmt),
    AlterTable(AlterTableStmt),
    RenameTable(RenameTableStmt),
    TruncateTable(TruncateTableStmt),
    OptimizeTable(OptimizeTableStmt),
    VacuumTable(VacuumTableStmt),
    VacuumDropTable(VacuumDropTableStmt),
    VacuumTemporaryFiles(VacuumTemporaryFiles),
    AnalyzeTable(AnalyzeTableStmt),
    ExistsTable(ExistsTableStmt),

    // Columns
    ShowColumns(ShowColumnsStmt),

    // Views
    CreateView(CreateViewStmt),
    AlterView(AlterViewStmt),
    DropView(DropViewStmt),
    ShowViews(ShowViewsStmt),
    DescribeView(DescribeViewStmt),

    // Streams
    CreateStream(CreateStreamStmt),
    DropStream(DropStreamStmt),
    ShowStreams(ShowStreamsStmt),
    DescribeStream(DescribeStreamStmt),

    // Indexes
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    RefreshIndex(RefreshIndexStmt),
    CreateInvertedIndex(CreateInvertedIndexStmt),
    DropInvertedIndex(DropInvertedIndexStmt),
    RefreshInvertedIndex(RefreshInvertedIndexStmt),

    // VirtualColumns
    CreateVirtualColumn(CreateVirtualColumnStmt),
    AlterVirtualColumn(AlterVirtualColumnStmt),
    DropVirtualColumn(DropVirtualColumnStmt),
    RefreshVirtualColumn(RefreshVirtualColumnStmt),
    ShowVirtualColumns(ShowVirtualColumnsStmt),

    // User
    ShowUsers,
    CreateUser(CreateUserStmt),
    AlterUser(AlterUserStmt),
    DropUser {
        #[drive(skip)]
        if_exists: bool,
        #[drive(skip)]
        user: UserIdentity,
    },
    ShowRoles,
    CreateRole {
        #[drive(skip)]
        if_not_exists: bool,
        #[drive(skip)]
        role_name: String,
    },
    DropRole {
        #[drive(skip)]
        if_exists: bool,
        #[drive(skip)]
        role_name: String,
    },
    Grant(GrantStmt),
    ShowGrants {
        #[drive(skip)]
        principal: Option<PrincipalIdentity>,
    },
    Revoke(RevokeStmt),

    // UDF
    CreateUDF(CreateUDFStmt),
    DropUDF {
        #[drive(skip)]
        if_exists: bool,
        udf_name: Identifier,
    },
    AlterUDF(AlterUDFStmt),

    // Stages
    CreateStage(CreateStageStmt),
    ShowStages,
    DropStage {
        #[drive(skip)]
        if_exists: bool,
        #[drive(skip)]
        stage_name: String,
    },
    DescribeStage {
        #[drive(skip)]
        stage_name: String,
    },
    RemoveStage {
        #[drive(skip)]
        location: String,
        #[drive(skip)]
        pattern: String,
    },
    ListStage {
        #[drive(skip)]
        location: String,
        #[drive(skip)]
        pattern: Option<String>,
    },
    // Connection
    CreateConnection(CreateConnectionStmt),
    DropConnection(DropConnectionStmt),
    DescribeConnection(DescribeConnectionStmt),
    ShowConnections(ShowConnectionsStmt),

    // UserDefinedFileFormat
    CreateFileFormat {
        #[drive(skip)]
        create_option: CreateOption,
        #[drive(skip)]
        name: String,
        file_format_options: FileFormatOptions,
    },
    DropFileFormat {
        #[drive(skip)]
        if_exists: bool,
        #[drive(skip)]
        name: String,
    },
    ShowFileFormats,
    Presign(PresignStmt),

    // data mask
    CreateDatamaskPolicy(CreateDatamaskPolicyStmt),
    DropDatamaskPolicy(DropDatamaskPolicyStmt),
    DescDatamaskPolicy(DescDatamaskPolicyStmt),

    // network policy
    CreateNetworkPolicy(CreateNetworkPolicyStmt),
    AlterNetworkPolicy(AlterNetworkPolicyStmt),
    DropNetworkPolicy(DropNetworkPolicyStmt),
    DescNetworkPolicy(DescNetworkPolicyStmt),
    ShowNetworkPolicies,

    // password policy
    CreatePasswordPolicy(CreatePasswordPolicyStmt),
    AlterPasswordPolicy(AlterPasswordPolicyStmt),
    DropPasswordPolicy(DropPasswordPolicyStmt),
    DescPasswordPolicy(DescPasswordPolicyStmt),
    ShowPasswordPolicies {
        show_options: Option<ShowOptions>,
    },

    // tasks
    CreateTask(CreateTaskStmt),
    AlterTask(AlterTaskStmt),
    ExecuteTask(ExecuteTaskStmt),
    DescribeTask(DescribeTaskStmt),
    DropTask(DropTaskStmt),
    ShowTasks(ShowTasksStmt),

    CreateDynamicTable(CreateDynamicTableStmt),

    // pipes
    CreatePipe(CreatePipeStmt),
    DescribePipe(DescribePipeStmt),
    DropPipe(DropPipeStmt),
    AlterPipe(AlterPipeStmt),

    // Transactions
    Begin,
    Commit,
    Abort,

    // Notifications
    CreateNotification(CreateNotificationStmt),
    AlterNotification(AlterNotificationStmt),
    DropNotification(DropNotificationStmt),
    DescribeNotification(DescribeNotificationStmt),

    // Stored procedures
    ExecuteImmediate(ExecuteImmediateStmt),

    // sequence
    CreateSequence(CreateSequenceStmt),
    DropSequence(DropSequenceStmt),

    // Set priority for query
    SetPriority {
        priority: Priority,
        #[drive(skip)]
        object_id: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementWithFormat {
    pub(crate) stmt: Statement,
    pub(crate) format: Option<String>,
}

impl Statement {
    pub fn to_mask_sql(&self) -> String {
        match self {
            Statement::CopyIntoTable(copy) => {
                let mut copy_clone = copy.clone();

                if let CopyIntoTableSource::Location(FileLocation::Uri(location)) =
                    &mut copy_clone.src
                {
                    location.connection = location.connection.mask()
                }
                format!("{}", Statement::CopyIntoTable(copy_clone))
            }
            Statement::CopyIntoLocation(copy) => {
                let mut copy_clone = copy.clone();

                if let FileLocation::Uri(location) = &mut copy_clone.dst {
                    location.connection = location.connection.mask()
                }
                format!("{}", Statement::CopyIntoLocation(copy_clone))
            }
            Statement::CreateStage(stage) => {
                let mut stage_clone = stage.clone();
                if let Some(location) = &mut stage_clone.location {
                    location.connection = location.connection.mask()
                }
                format!("{}", Statement::CreateStage(stage_clone))
            }
            Statement::AttachTable(attach) => {
                let mut attach_clone = attach.clone();
                attach_clone.uri_location.connection = attach_clone.uri_location.connection.mask();
                format!("{}", Statement::AttachTable(attach_clone))
            }
            _ => format!("{}", self),
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Explain {
                options,
                kind,
                query,
            } => {
                write!(f, "EXPLAIN")?;
                if !options.is_empty() {
                    write!(
                        f,
                        "({})",
                        options
                            .iter()
                            .map(|opt| {
                                match opt {
                                    ExplainOption::Verbose => "VERBOSE",
                                    ExplainOption::Logical => "LOGICAL",
                                    ExplainOption::Optimized => "OPTIMIZED",
                                }
                            })
                            .join(", ")
                    )?;
                }
                match *kind {
                    ExplainKind::Ast(_) => write!(f, " AST")?,
                    ExplainKind::Syntax(_) => write!(f, " SYNTAX")?,
                    ExplainKind::Graph => write!(f, " GRAPH")?,
                    ExplainKind::Pipeline => write!(f, " PIPELINE")?,
                    ExplainKind::Fragments => write!(f, " FRAGMENTS")?,
                    ExplainKind::Raw => write!(f, " RAW")?,
                    ExplainKind::Optimized => write!(f, " Optimized")?,
                    ExplainKind::Plan => (),
                    ExplainKind::AnalyzePlan => write!(f, " ANALYZE")?,
                    ExplainKind::Join => write!(f, " JOIN")?,
                    ExplainKind::Memo(_) => write!(f, " MEMO")?,
                }
                write!(f, " {query}")?;
            }
            Statement::ExplainAnalyze { query } => {
                write!(f, "EXPLAIN ANALYZE {query}")?;
            }
            Statement::Query(stmt) => write!(f, "{stmt}")?,
            Statement::Insert(stmt) => write!(f, "{stmt}")?,
            Statement::InsertMultiTable(insert_multi_table) => write!(f, "{insert_multi_table}")?,
            Statement::Replace(stmt) => write!(f, "{stmt}")?,
            Statement::MergeInto(stmt) => write!(f, "{stmt}")?,
            Statement::Delete(stmt) => write!(f, "{stmt}")?,
            Statement::Update(stmt) => write!(f, "{stmt}")?,
            Statement::CopyIntoTable(stmt) => write!(f, "{stmt}")?,
            Statement::CopyIntoLocation(stmt) => write!(f, "{stmt}")?,
            Statement::ShowSettings { show_options } => {
                write!(f, "SHOW SETTINGS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowProcessList { show_options } => {
                write!(f, "SHOW PROCESSLIST")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowMetrics { show_options } => {
                write!(f, "SHOW METRICS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowEngines { show_options } => {
                write!(f, "SHOW ENGINES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowIndexes { show_options } => {
                write!(f, "SHOW INDEXES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowFunctions { show_options } => {
                write!(f, "SHOW FUNCTIONS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowUserFunctions { show_options } => {
                write!(f, "SHOW USER FUNCTIONS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowTableFunctions { show_options } => {
                write!(f, "SHOW TABLE_FUNCTIONS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowLocks(stmt) => write!(f, "{stmt}")?,
            Statement::KillStmt {
                kill_target,
                object_id,
            } => {
                write!(f, "KILL")?;
                match *kill_target {
                    KillTarget::Query => write!(f, " QUERY")?,
                    KillTarget::Connection => write!(f, " CONNECTION")?,
                }
                write!(f, " '{object_id}'")?;
            }
            Statement::SetVariable {
                is_global,
                variable,
                value,
            } => {
                write!(f, "SET ")?;
                if *is_global {
                    write!(f, "GLOBAL ")?;
                }
                write!(f, "{variable} = {value}")?;
            }
            Statement::UnSetVariable(stmt) => write!(f, "{stmt}")?,
            Statement::SetRole {
                is_default,
                role_name,
            } => {
                write!(f, "SET ")?;
                if *is_default {
                    write!(f, "DEFAULT ")?;
                }
                write!(f, "ROLE '{role_name}'")?;
            }
            Statement::SetSecondaryRoles { option } => {
                write!(f, "SET SECONDARY ROLES ")?;
                match option {
                    SecondaryRolesOption::None => write!(f, "NONE")?,
                    SecondaryRolesOption::All => write!(f, "ALL")?,
                }
            }
            Statement::ShowCatalogs(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::CreateCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::DropCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDatabases(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UndropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::AlterDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UseDatabase { database } => write!(f, "USE {database}")?,
            Statement::ShowTables(stmt) => write!(f, "{stmt}")?,
            Statement::ShowColumns(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTablesStatus(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDropTables(stmt) => write!(f, "{stmt}")?,
            Statement::AttachTable(stmt) => write!(f, "{stmt}")?,
            Statement::CreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DropTable(stmt) => write!(f, "{stmt}")?,
            Statement::UndropTable(stmt) => write!(f, "{stmt}")?,
            Statement::AlterTable(stmt) => write!(f, "{stmt}")?,
            Statement::RenameTable(stmt) => write!(f, "{stmt}")?,
            Statement::TruncateTable(stmt) => write!(f, "{stmt}")?,
            Statement::OptimizeTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumDropTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumTemporaryFiles(stmt) => write!(f, "{stmt}")?,
            Statement::AnalyzeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ExistsTable(stmt) => write!(f, "{stmt}")?,
            Statement::CreateView(stmt) => write!(f, "{stmt}")?,
            Statement::AlterView(stmt) => write!(f, "{stmt}")?,
            Statement::DropView(stmt) => write!(f, "{stmt}")?,
            Statement::ShowViews(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeView(stmt) => write!(f, "{stmt}")?,
            Statement::CreateStream(stmt) => write!(f, "{stmt}")?,
            Statement::DropStream(stmt) => write!(f, "{stmt}")?,
            Statement::ShowStreams(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeStream(stmt) => write!(f, "{stmt}")?,
            Statement::CreateIndex(stmt) => write!(f, "{stmt}")?,
            Statement::DropIndex(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshIndex(stmt) => write!(f, "{stmt}")?,
            Statement::CreateInvertedIndex(stmt) => write!(f, "{stmt}")?,
            Statement::DropInvertedIndex(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshInvertedIndex(stmt) => write!(f, "{stmt}")?,
            Statement::CreateVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::AlterVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::DropVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::ShowVirtualColumns(stmt) => write!(f, "{stmt}")?,
            Statement::ShowUsers => write!(f, "SHOW USERS")?,
            Statement::ShowRoles => write!(f, "SHOW ROLES")?,
            Statement::CreateUser(stmt) => write!(f, "{stmt}")?,
            Statement::AlterUser(stmt) => write!(f, "{stmt}")?,
            Statement::DropUser { if_exists, user } => {
                write!(f, "DROP USER")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {}", user.display())?;
            }
            Statement::CreateRole {
                if_not_exists,
                role_name: role,
            } => {
                write!(f, "CREATE ROLE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " '{}'", escape_string_with_quote(role, Some('\'')))?;
            }
            Statement::DropRole {
                if_exists,
                role_name: role,
            } => {
                write!(f, "DROP ROLE")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " '{role}'")?;
            }
            Statement::Grant(stmt) => write!(f, "{stmt}")?,
            Statement::ShowGrants { principal } => {
                write!(f, "SHOW GRANTS")?;
                if let Some(principal) = principal {
                    write!(f, " FOR")?;
                    write!(f, "{principal}")?;
                }
            }
            Statement::Revoke(stmt) => write!(f, "{stmt}")?,
            Statement::CreateUDF(stmt) => write!(f, "{stmt}")?,
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => {
                write!(f, "DROP FUNCTION")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {udf_name}")?;
            }
            Statement::AlterUDF(stmt) => write!(f, "{stmt}")?,
            Statement::ListStage { location, pattern } => {
                write!(f, "LIST @{location}")?;
                if let Some(pattern) = pattern {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
            }
            Statement::ShowStages => write!(f, "SHOW STAGES")?,
            Statement::DropStage {
                if_exists,
                stage_name,
            } => {
                write!(f, "DROP STAGE")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {stage_name}")?;
            }
            Statement::CreateStage(stmt) => write!(f, "{stmt}")?,
            Statement::RemoveStage { location, pattern } => {
                write!(f, "REMOVE @{location}")?;
                if !pattern.is_empty() {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
            }
            Statement::DescribeStage { stage_name } => write!(f, "DESC STAGE {stage_name}")?,
            Statement::CreateFileFormat {
                create_option,
                name,
                file_format_options,
            } => {
                write!(f, "CREATE")?;
                if let CreateOption::CreateOrReplace = create_option {
                    write!(f, " OR REPLACE")?;
                }
                write!(f, " FILE FORMAT")?;
                if let CreateOption::CreateIfNotExists = create_option {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {name}")?;
                write!(f, " {file_format_options}")?;
            }
            Statement::DropFileFormat { if_exists, name } => {
                write!(f, "DROP FILE FORMAT")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")?;
            }
            Statement::ShowFileFormats => write!(f, "SHOW FILE FORMATS")?,
            Statement::Call(stmt) => write!(f, "{stmt}")?,
            Statement::Presign(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::CreateNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::AlterNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::ShowNetworkPolicies => write!(f, "SHOW NETWORK POLICIES")?,
            Statement::CreatePasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::AlterPasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropPasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescPasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::ShowPasswordPolicies { show_options } => {
                write!(f, "SHOW PASSWORD POLICIES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::CreateTask(stmt) => write!(f, "{stmt}")?,
            Statement::AlterTask(stmt) => write!(f, "{stmt}")?,
            Statement::ExecuteTask(stmt) => write!(f, "{stmt}")?,
            Statement::DropTask(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTasks(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeTask(stmt) => write!(f, "{stmt}")?,
            Statement::CreatePipe(stmt) => write!(f, "{stmt}")?,
            Statement::DescribePipe(stmt) => write!(f, "{stmt}")?,
            Statement::DropPipe(stmt) => write!(f, "{stmt}")?,
            Statement::AlterPipe(stmt) => write!(f, "{stmt}")?,
            Statement::CreateConnection(stmt) => write!(f, "{stmt}")?,
            Statement::DropConnection(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeConnection(stmt) => write!(f, "{stmt}")?,
            Statement::ShowConnections(stmt) => write!(f, "{stmt}")?,
            Statement::Begin => write!(f, "BEGIN")?,
            Statement::Commit => write!(f, "COMMIT")?,
            Statement::Abort => write!(f, "ABORT")?,
            Statement::CreateNotification(stmt) => write!(f, "{stmt}")?,
            Statement::AlterNotification(stmt) => write!(f, "{stmt}")?,
            Statement::DropNotification(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeNotification(stmt) => write!(f, "{stmt}")?,
            Statement::ExecuteImmediate(stmt) => write!(f, "{stmt}")?,
            Statement::CreateSequence(stmt) => write!(f, "{stmt}")?,
            Statement::DropSequence(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDynamicTable(stmt) => write!(f, "{stmt}")?,
            Statement::SetPriority {
                priority,
                object_id,
            } => {
                write!(f, "SET PRIORITY")?;
                write!(f, " {priority}")?;
                write!(f, " '{object_id}'")?;
            }
        }
        Ok(())
    }
}
