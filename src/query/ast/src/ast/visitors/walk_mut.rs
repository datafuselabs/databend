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

#![allow(deprecated)]

use super::visitor_mut::VisitorMut;
use crate::ast::*;

pub fn walk_expr_mut<V: VisitorMut>(visitor: &mut V, expr: &mut Expr) {
    match expr {
        Expr::ColumnRef {
            span,
            column:
                ColumnRef {
                    database,
                    table,
                    column,
                },
        } => visitor.visit_column_ref(*span, database, table, column),
        Expr::IsNull { span, expr, not } => visitor.visit_is_null(*span, expr, *not),
        Expr::IsDistinctFrom {
            span,
            left,
            right,
            not,
        } => visitor.visit_is_distinct_from(*span, left, right, *not),
        Expr::InList {
            span,
            expr,
            list,
            not,
        } => visitor.visit_in_list(*span, expr, list, *not),
        Expr::InSubquery {
            span,
            expr,
            subquery,
            not,
        } => visitor.visit_in_subquery(*span, expr, subquery, *not),
        Expr::Between {
            span,
            expr,
            low,
            high,
            not,
        } => visitor.visit_between(*span, expr, low, high, *not),
        Expr::BinaryOp {
            span,
            op,
            left,
            right,
        } => visitor.visit_binary_op(*span, op, left, right),
        Expr::JsonOp {
            span,
            op,
            left,
            right,
        } => visitor.visit_json_op(*span, op, left, right),
        Expr::UnaryOp { span, op, expr } => visitor.visit_unary_op(*span, op, expr),
        Expr::Cast {
            span,
            expr,
            target_type,
            pg_style,
        } => visitor.visit_cast(*span, expr, target_type, *pg_style),
        Expr::TryCast {
            span,
            expr,
            target_type,
        } => visitor.visit_try_cast(*span, expr, target_type),
        Expr::Extract { span, kind, expr } => visitor.visit_extract(*span, kind, expr),
        Expr::DatePart { span, kind, expr } => visitor.visit_extract(*span, kind, expr),
        Expr::Position {
            span,
            substr_expr,
            str_expr,
        } => visitor.visit_position(*span, substr_expr, str_expr),
        Expr::Substring {
            span,
            expr,
            substring_from,
            substring_for,
        } => visitor.visit_substring(*span, expr, substring_from, substring_for),
        Expr::Trim {
            span,
            expr,
            trim_where,
        } => visitor.visit_trim(*span, expr, trim_where),
        Expr::Literal { span, value } => visitor.visit_literal(*span, value),
        Expr::CountAll { span, window } => visitor.visit_count_all(*span, window),
        Expr::Tuple { span, exprs } => visitor.visit_tuple(*span, exprs),
        Expr::FunctionCall {
            span,
            func:
                FunctionCall {
                    distinct,
                    name,
                    args,
                    params,
                    window,
                    lambda,
                },
        } => visitor.visit_function_call(*span, *distinct, name, args, params, window, lambda),
        Expr::Case {
            span,
            operand,
            conditions,
            results,
            else_result,
        } => visitor.visit_case_when(*span, operand, conditions, results, else_result),
        Expr::Exists {
            span,
            not,
            subquery,
        } => visitor.visit_exists(*span, *not, subquery),
        Expr::Subquery {
            span,
            modifier,
            subquery,
        } => visitor.visit_subquery(*span, modifier, subquery),
        Expr::MapAccess {
            span,
            expr,
            accessor,
        } => visitor.visit_map_access(*span, expr, accessor),
        Expr::Array { span, exprs } => visitor.visit_array(*span, exprs),
        Expr::Map { span, kvs } => visitor.visit_map(*span, kvs),
        Expr::Interval { span, expr, unit } => visitor.visit_interval(*span, expr, unit),
        Expr::DateAdd {
            span,
            unit,
            interval,
            date,
        } => visitor.visit_date_add(*span, unit, interval, date),
        Expr::DateSub {
            span,
            date,
            interval,
            unit,
        } => visitor.visit_date_sub(*span, unit, interval, date),
        Expr::DateTrunc { span, unit, date } => visitor.visit_date_trunc(*span, unit, date),
        Expr::Hole { .. } => {}
    }
}

pub fn walk_identifier_mut<V: VisitorMut>(visitor: &mut V, ident: &mut Identifier) {
    visitor.visit_identifier(ident);
}

pub fn walk_column_id_mut<V: VisitorMut>(visitor: &mut V, ident: &mut ColumnID) {
    visitor.visit_column_id(ident);
}

pub fn walk_query_mut<V: VisitorMut>(visitor: &mut V, query: &mut Query) {
    let Query {
        with,
        body,
        order_by,
        limit,
        offset,
        ..
    } = query;

    if let Some(with) = with {
        visitor.visit_with(with);
    }
    visitor.visit_set_expr(body);
    for order_by in order_by {
        visitor.visit_order_by(order_by);
    }
    for limit in limit {
        visitor.visit_expr(limit);
    }
    if let Some(offset) = offset {
        visitor.visit_expr(offset);
    }
}

pub fn walk_set_expr_mut<V: VisitorMut>(visitor: &mut V, set_expr: &mut SetExpr) {
    match set_expr {
        SetExpr::Select(select) => {
            visitor.visit_select_stmt(select);
        }
        SetExpr::Query(query) => {
            visitor.visit_query(query);
        }
        SetExpr::SetOperation(op) => {
            visitor.visit_set_operation(op);
        }
        SetExpr::Values { values, .. } => {
            for row_values in values {
                for value in row_values {
                    visitor.visit_expr(value);
                }
            }
        }
    }
}

pub fn walk_window_definition_mut<V: VisitorMut>(
    visitor: &mut V,
    window_definition: &mut WindowDefinition,
) {
    let WindowDefinition { name, spec: window } = window_definition;

    visitor.visit_identifier(name);

    let WindowSpec {
        partition_by,
        order_by,
        window_frame,
        ..
    } = window;

    for expr in partition_by {
        visitor.visit_expr(expr);
    }

    for order_by in order_by {
        visitor.visit_order_by(order_by);
    }

    if let Some(frame) = window_frame {
        visitor.visit_frame_bound(&mut frame.start_bound);
        visitor.visit_frame_bound(&mut frame.end_bound);
    }
}

pub fn walk_select_target_mut<V: VisitorMut>(visitor: &mut V, target: &mut SelectTarget) {
    match target {
        SelectTarget::AliasedExpr { expr, alias } => {
            visitor.visit_expr(expr);
            if let Some(alias) = alias {
                visitor.visit_identifier(alias);
            }
        }
        SelectTarget::StarColumns {
            qualified: names,
            column_filter,
        } => {
            for indirection in names {
                match indirection {
                    Indirection::Identifier(ident) => {
                        visitor.visit_identifier(ident);
                    }
                    Indirection::Star(_) => {}
                }
            }

            if let Some(col_filter) = column_filter {
                match col_filter {
                    ColumnFilter::Excludes(excludes) => {
                        for ident in excludes.iter_mut() {
                            visitor.visit_identifier(ident);
                        }
                    }
                    ColumnFilter::Lambda(lambda) => {
                        visitor.visit_expr(lambda.expr.as_mut());
                    }
                }
            }
        }
    }
}

pub fn walk_table_reference_mut<V: VisitorMut>(visitor: &mut V, table_ref: &mut TableReference) {
    match table_ref {
        TableReference::Table {
            catalog,
            database,
            table,
            alias,
            temporal,
            ..
        } => {
            if let Some(catalog) = catalog {
                visitor.visit_identifier(catalog);
            }

            if let Some(database) = database {
                visitor.visit_identifier(database);
            }

            visitor.visit_identifier(table);

            if let Some(alias) = alias {
                visitor.visit_identifier(&mut alias.name);
            }

            if let Some(temporal) = temporal {
                visitor.visit_temporal_clause(temporal);
            }
        }
        TableReference::Subquery {
            subquery, alias, ..
        } => {
            visitor.visit_query(subquery);
            if let Some(alias) = alias {
                visitor.visit_identifier(&mut alias.name);
            }
        }
        TableReference::TableFunction {
            name,
            params,
            alias,
            ..
        } => {
            visitor.visit_identifier(name);
            for param in params {
                visitor.visit_expr(param);
            }
            if let Some(alias) = alias {
                visitor.visit_identifier(&mut alias.name);
            }
        }
        TableReference::Join { join, .. } => {
            visitor.visit_join(join);
        }
        TableReference::Location { .. } => {}
    }
}

pub fn walk_time_travel_point_mut<V: VisitorMut>(visitor: &mut V, time: &mut TimeTravelPoint) {
    match time {
        TimeTravelPoint::Snapshot(_) => {}
        TimeTravelPoint::Timestamp(expr) => visitor.visit_expr(expr),
        TimeTravelPoint::Offset(expr) => visitor.visit_expr(expr),
        TimeTravelPoint::Stream {
            catalog,
            database,
            name,
        } => {
            if let Some(catalog) = catalog {
                visitor.visit_identifier(catalog);
            }

            if let Some(database) = database {
                visitor.visit_identifier(database);
            }

            visitor.visit_identifier(name);
        }
    }
}

pub fn walk_temporal_clause_mut<V: VisitorMut>(visitor: &mut V, clause: &mut TemporalClause) {
    match clause {
        TemporalClause::TimeTravel(point) => visitor.visit_time_travel_point(point),
        TemporalClause::Changes(ChangesInterval {
            at_point,
            end_point,
            ..
        }) => {
            visitor.visit_time_travel_point(at_point);
            if let Some(point) = end_point {
                visitor.visit_time_travel_point(point);
            }
        }
    }
}

pub fn walk_join_condition_mut<V: VisitorMut>(visitor: &mut V, join_cond: &mut JoinCondition) {
    match join_cond {
        JoinCondition::On(expr) => visitor.visit_expr(expr),
        JoinCondition::Using(using) => {
            for ident in using.iter_mut() {
                visitor.visit_identifier(ident);
            }
        }
        JoinCondition::Natural => {}
        JoinCondition::None => {}
    }
}

pub fn walk_cte_mut<V: VisitorMut>(visitor: &mut V, cte: &mut CTE) {
    let CTE { alias, query, .. } = cte;

    visitor.visit_identifier(&mut alias.name);
    visitor.visit_query(query);
}

pub fn walk_statement_mut<V: VisitorMut>(visitor: &mut V, statement: &mut Statement) {
    match statement {
        Statement::Explain {
            kind,
            options,
            query,
        } => visitor.visit_explain(kind, options, &mut *query),
        Statement::ExplainAnalyze { query } => visitor.visit_statement(&mut *query),
        Statement::Query(query) => visitor.visit_query(&mut *query),
        Statement::Insert(insert) => visitor.visit_insert(insert),
        Statement::Replace(replace) => visitor.visit_replace(replace),
        Statement::MergeInto(merge_into) => visitor.visit_merge_into(merge_into),
        Statement::Delete(delete) => visitor.visit_delete(delete),
        Statement::Update(update) => visitor.visit_update(update),
        Statement::CopyIntoLocation(stmt) => visitor.visit_copy_into_location(stmt),
        Statement::CopyIntoTable(stmt) => visitor.visit_copy_into_table(stmt),
        Statement::ShowSettings { show_options } => visitor.visit_show_settings(show_options),
        Statement::ShowProcessList { show_options } => {
            visitor.visit_show_process_list(show_options)
        }
        Statement::ShowMetrics { show_options } => visitor.visit_show_metrics(show_options),
        Statement::ShowEngines { show_options } => visitor.visit_show_engines(show_options),
        Statement::ShowFunctions { show_options } => visitor.visit_show_functions(show_options),
        Statement::ShowUserFunctions { show_options } => {
            visitor.visit_show_user_functions(show_options)
        }
        Statement::ShowIndexes { show_options } => visitor.visit_show_indexes(show_options),
        Statement::ShowLocks(stmt) => visitor.visit_show_locks(stmt),
        Statement::ShowTableFunctions { show_options } => {
            visitor.visit_show_table_functions(show_options)
        }
        Statement::KillStmt {
            kill_target,
            object_id,
        } => visitor.visit_kill(kill_target, object_id),
        Statement::SetVariable {
            is_global,
            variable,
            value,
        } => visitor.visit_set_variable(*is_global, variable, value),
        Statement::UnSetVariable(stmt) => visitor.visit_unset_variable(stmt),
        Statement::SetRole {
            is_default,
            role_name,
        } => visitor.visit_set_role(*is_default, role_name),
        Statement::SetSecondaryRoles { option } => visitor.visit_set_secondary_roles(option),
        Statement::ShowCatalogs(stmt) => visitor.visit_show_catalogs(stmt),
        Statement::ShowCreateCatalog(stmt) => visitor.visit_show_create_catalog(stmt),
        Statement::CreateCatalog(stmt) => visitor.visit_create_catalog(stmt),
        Statement::DropCatalog(stmt) => visitor.visit_drop_catalog(stmt),
        Statement::ShowDatabases(stmt) => visitor.visit_show_databases(stmt),
        Statement::ShowCreateDatabase(stmt) => visitor.visit_show_create_databases(stmt),
        Statement::CreateDatabase(stmt) => visitor.visit_create_database(stmt),
        Statement::DropDatabase(stmt) => visitor.visit_drop_database(stmt),
        Statement::UndropDatabase(stmt) => visitor.visit_undrop_database(stmt),
        Statement::AlterDatabase(stmt) => visitor.visit_alter_database(stmt),
        Statement::UseDatabase { database } => visitor.visit_use_database(database),
        Statement::ShowTables(stmt) => visitor.visit_show_tables(stmt),
        Statement::ShowColumns(stmt) => visitor.visit_show_columns(stmt),
        Statement::ShowCreateTable(stmt) => visitor.visit_show_create_table(stmt),
        Statement::DescribeTable(stmt) => visitor.visit_describe_table(stmt),
        Statement::ShowTablesStatus(stmt) => visitor.visit_show_tables_status(stmt),
        Statement::ShowDropTables(stmt) => visitor.visit_show_drop_tables(stmt),
        Statement::CreateTable(stmt) => visitor.visit_create_table(stmt),
        Statement::DropTable(stmt) => visitor.visit_drop_table(stmt),
        Statement::UndropTable(stmt) => visitor.visit_undrop_table(stmt),
        Statement::AlterTable(stmt) => visitor.visit_alter_table(stmt),
        Statement::RenameTable(stmt) => visitor.visit_rename_table(stmt),
        Statement::TruncateTable(stmt) => visitor.visit_truncate_table(stmt),
        Statement::OptimizeTable(stmt) => visitor.visit_optimize_table(stmt),
        Statement::VacuumTable(stmt) => visitor.visit_vacuum_table(stmt),
        Statement::VacuumDropTable(stmt) => visitor.visit_vacuum_drop_table(stmt),
        Statement::VacuumTemporaryFiles(stmt) => visitor.visit_vacuum_temporary_files(stmt),
        Statement::AnalyzeTable(stmt) => visitor.visit_analyze_table(stmt),
        Statement::ExistsTable(stmt) => visitor.visit_exists_table(stmt),
        Statement::CreateView(stmt) => visitor.visit_create_view(stmt),
        Statement::AlterView(stmt) => visitor.visit_alter_view(stmt),
        Statement::DropView(stmt) => visitor.visit_drop_view(stmt),
        Statement::ShowViews(stmt) => visitor.visit_show_views(stmt),
        Statement::DescribeView(stmt) => visitor.visit_describe_view(stmt),
        Statement::CreateStream(stmt) => visitor.visit_create_stream(stmt),
        Statement::DropStream(stmt) => visitor.visit_drop_stream(stmt),
        Statement::ShowStreams(stmt) => visitor.visit_show_streams(stmt),
        Statement::DescribeStream(stmt) => visitor.visit_describe_stream(stmt),
        Statement::CreateIndex(stmt) => visitor.visit_create_index(stmt),
        Statement::DropIndex(stmt) => visitor.visit_drop_index(stmt),
        Statement::RefreshIndex(stmt) => visitor.visit_refresh_index(stmt),
        Statement::CreateInvertedIndex(stmt) => visitor.visit_create_inverted_index(stmt),
        Statement::DropInvertedIndex(stmt) => visitor.visit_drop_inverted_index(stmt),
        Statement::RefreshInvertedIndex(stmt) => visitor.visit_refresh_inverted_index(stmt),
        Statement::CreateVirtualColumn(stmt) => visitor.visit_create_virtual_column(stmt),
        Statement::AlterVirtualColumn(stmt) => visitor.visit_alter_virtual_column(stmt),
        Statement::DropVirtualColumn(stmt) => visitor.visit_drop_virtual_column(stmt),
        Statement::RefreshVirtualColumn(stmt) => visitor.visit_refresh_virtual_column(stmt),
        Statement::ShowVirtualColumns(stmt) => visitor.visit_show_virtual_columns(stmt),
        Statement::ShowUsers => visitor.visit_show_users(),
        Statement::ShowRoles => visitor.visit_show_roles(),
        Statement::CreateUser(stmt) => visitor.visit_create_user(stmt),
        Statement::AlterUser(stmt) => visitor.visit_alter_user(stmt),
        Statement::DropUser { if_exists, user } => visitor.visit_drop_user(*if_exists, user),
        Statement::CreateRole {
            if_not_exists,
            role_name,
        } => visitor.visit_create_role(*if_not_exists, role_name),
        Statement::DropRole {
            if_exists,
            role_name,
        } => visitor.visit_drop_role(*if_exists, role_name),
        Statement::Grant(stmt) => visitor.visit_grant(stmt),
        Statement::ShowGrants { principal } => visitor.visit_show_grant(principal),
        Statement::Revoke(stmt) => visitor.visit_revoke(stmt),
        Statement::CreateUDF(stmt) => visitor.visit_create_udf(stmt),
        Statement::DropUDF {
            if_exists,
            udf_name,
        } => visitor.visit_drop_udf(*if_exists, udf_name),
        Statement::AlterUDF(stmt) => visitor.visit_alter_udf(stmt),
        Statement::ListStage { location, pattern } => visitor.visit_list_stage(location, pattern),
        Statement::ShowStages => visitor.visit_show_stages(),
        Statement::DropStage {
            if_exists,
            stage_name,
        } => visitor.visit_drop_stage(*if_exists, stage_name),
        Statement::CreateStage(stmt) => visitor.visit_create_stage(stmt),
        Statement::RemoveStage { location, pattern } => {
            visitor.visit_remove_stage(location, pattern)
        }
        Statement::DescribeStage { stage_name } => visitor.visit_describe_stage(stage_name),
        Statement::CreateFileFormat {
            create_option,
            name,
            file_format_options,
        } => visitor.visit_create_file_format(create_option, name, file_format_options),
        Statement::DropFileFormat { if_exists, name } => {
            visitor.visit_drop_file_format(*if_exists, name)
        }
        Statement::ShowFileFormats => visitor.visit_show_file_formats(),
        Statement::Call(stmt) => visitor.visit_call(stmt),
        Statement::Presign(stmt) => visitor.visit_presign(stmt),
        Statement::CreateDatamaskPolicy(stmt) => visitor.visit_create_data_mask_policy(stmt),
        Statement::DropDatamaskPolicy(stmt) => visitor.visit_drop_data_mask_policy(stmt),
        Statement::DescDatamaskPolicy(stmt) => visitor.visit_desc_data_mask_policy(stmt),
        Statement::AttachTable(_) => {}
        Statement::CreateNetworkPolicy(stmt) => visitor.visit_create_network_policy(stmt),
        Statement::AlterNetworkPolicy(stmt) => visitor.visit_alter_network_policy(stmt),
        Statement::DropNetworkPolicy(stmt) => visitor.visit_drop_network_policy(stmt),
        Statement::DescNetworkPolicy(stmt) => visitor.visit_desc_network_policy(stmt),
        Statement::ShowNetworkPolicies => visitor.visit_show_network_policies(),
        Statement::CreatePasswordPolicy(stmt) => visitor.visit_create_password_policy(stmt),
        Statement::AlterPasswordPolicy(stmt) => visitor.visit_alter_password_policy(stmt),
        Statement::DropPasswordPolicy(stmt) => visitor.visit_drop_password_policy(stmt),
        Statement::DescPasswordPolicy(stmt) => visitor.visit_desc_password_policy(stmt),
        Statement::ShowPasswordPolicies { show_options } => {
            visitor.visit_show_password_policies(show_options)
        }

        Statement::CreateTask(stmt) => visitor.visit_create_task(stmt),
        Statement::ExecuteTask(stmt) => visitor.visit_execute_task(stmt),
        Statement::DropTask(stmt) => visitor.visit_drop_task(stmt),
        Statement::AlterTask(stmt) => visitor.visit_alter_task(stmt),
        Statement::ShowTasks(stmt) => visitor.visit_show_tasks(stmt),
        Statement::DescribeTask(stmt) => visitor.visit_describe_task(stmt),

        Statement::CreateDynamicTable(stmt) => visitor.visit_create_dynamic_table(stmt),

        Statement::CreateConnection(stmt) => visitor.visit_create_connection(stmt),
        Statement::DropConnection(stmt) => visitor.visit_drop_connection(stmt),
        Statement::DescribeConnection(stmt) => visitor.visit_describe_connection(stmt),
        Statement::ShowConnections(stmt) => visitor.visit_show_connections(stmt),

        Statement::CreatePipe(_) => todo!(),
        Statement::AlterPipe(_) => todo!(),
        Statement::DropPipe(_) => todo!(),
        Statement::DescribePipe(_) => todo!(),
        Statement::Begin => {}
        Statement::Commit => {}
        Statement::Abort => {}
        Statement::CreateNotification(stmt) => visitor.visit_create_notification(stmt),
        Statement::AlterNotification(stmt) => visitor.visit_alter_notification(stmt),
        Statement::DropNotification(stmt) => visitor.visit_drop_notification(stmt),
        Statement::DescribeNotification(stmt) => visitor.visit_describe_notification(stmt),
        Statement::InsertMultiTable(_) => {}
        Statement::ExecuteImmediate(_) => {}
        Statement::CreateSequence(stmt) => visitor.visit_create_sequence(stmt),
        Statement::DropSequence(stmt) => visitor.visit_drop_sequence(stmt),
        Statement::SetPriority {
            priority,
            object_id,
        } => visitor.visit_set_priority(priority, object_id),
    }
}
