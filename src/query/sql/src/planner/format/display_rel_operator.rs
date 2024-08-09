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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::optimizer::SExpr;
use crate::planner::format::display::DefaultOperatorHumanizer;
use crate::planner::format::display::IdHumanizer;
use crate::planner::format::display::TreeHumanizer;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::EvalScalar;
use crate::plans::Exchange;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Limit;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::Scan;
use crate::plans::Sort;
use crate::plans::Window;
use crate::IndexType;

impl SExpr {
    pub fn to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
        &self,
        id_humanizer: &I,
        verbose: bool,
    ) -> Result<FormatTreeNode> {
        let operator_humanizer = DefaultOperatorHumanizer;
        let tree_humanizer = TreeHumanizer::new(id_humanizer, &operator_humanizer, verbose);
        tree_humanizer.humanize_s_expr(self)
    }
}

pub fn format_scalar(scalar: &ScalarExpr) -> String {
    match scalar {
        ScalarExpr::BoundColumnRef(column_ref) => {
            if let Some(table_name) = &column_ref.column.table_name {
                format!(
                    "{}.{} (#{})",
                    table_name, column_ref.column.column_name, column_ref.column.index
                )
            } else {
                format!(
                    "{} (#{})",
                    column_ref.column.column_name, column_ref.column.index
                )
            }
        }
        ScalarExpr::ConstantExpr(constant) => constant.value.to_string(),
        ScalarExpr::WindowFunction(win) => win.display_name.clone(),
        ScalarExpr::AggregateFunction(agg) => agg.display_name.clone(),
        ScalarExpr::LambdaFunction(lambda) => {
            let args = lambda
                .args
                .iter()
                .map(format_scalar)
                .collect::<Vec<String>>()
                .join(", ");
            format!(
                "{}({}, {})",
                &lambda.func_name, args, &lambda.lambda_display,
            )
        }
        ScalarExpr::FunctionCall(func) => {
            format!(
                "{}({})",
                &func.func_name,
                func.arguments
                    .iter()
                    .map(format_scalar)
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        ScalarExpr::CastExpr(cast) => {
            format!(
                "CAST({} AS {})",
                format_scalar(&cast.argument),
                cast.target_type
            )
        }
        ScalarExpr::SubqueryExpr(_) => "SUBQUERY".to_string(),
        ScalarExpr::UDFCall(udf) => {
            format!(
                "{}({})",
                &udf.func_name,
                udf.arguments
                    .iter()
                    .map(format_scalar)
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        ScalarExpr::UDFLambdaCall(udf) => {
            format!("{}({})", &udf.func_name, format_scalar(&udf.scalar))
        }
        ScalarExpr::AsyncFunctionCall(table_func) => table_func.display_name.to_string(),
    }
}

pub fn format_aggregate(op: &Aggregate) -> String {
    format!("Aggregate({})", match &op.mode {
        AggregateMode::Partial => "Partial",
        AggregateMode::Final => "Final",
        AggregateMode::Initial => "Initial",
    })
}

pub fn format_exchange(op: &Exchange) -> String {
    match op {
        Exchange::Hash(_) => "Exchange(Hash)".to_string(),
        Exchange::Broadcast => "Exchange(Broadcast)".to_string(),
        Exchange::Merge => "Exchange(Merge)".to_string(),
        Exchange::MergeSort => "Exchange(MergeSort)".to_string(),
    }
}

/// Build `FormatTreeNode` for a `RelOperator`, which may returns a tree structure instead of
/// a single node.
pub(super) fn to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    id_humanizer: &I,
    op: &RelOperator,
) -> FormatTreeNode {
    match op {
        RelOperator::Join(op) => join_to_format_tree(id_humanizer, op),
        RelOperator::Scan(op) => scan_to_format_tree(id_humanizer, op),
        RelOperator::EvalScalar(op) => eval_scalar_to_format_tree(id_humanizer, op),
        RelOperator::Filter(op) => filter_to_format_tree(id_humanizer, op),
        RelOperator::Aggregate(op) => aggregate_to_format_tree(id_humanizer, op),
        RelOperator::Window(op) => window_to_format_tree(id_humanizer, op),
        RelOperator::Sort(op) => sort_to_format_tree(id_humanizer, op),
        RelOperator::Limit(op) => limit_to_format_tree(id_humanizer, op),
        RelOperator::Exchange(op) => exchange_to_format_tree(id_humanizer, op),
        RelOperator::AsyncFunction(op) => {
            FormatTreeNode::with_children(format!("AsyncFunction: {}", op.display_name), vec![])
        }

        _ => FormatTreeNode::with_children(format!("{:?}", op), vec![]),
    }
}

fn scan_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    id_humanizer: &I,
    op: &Scan,
) -> FormatTreeNode {
    FormatTreeNode::with_children("Scan".to_string(), vec![
        FormatTreeNode::new(format!(
            "table: {}",
            id_humanizer.humanize_table_id(op.table_index)
        )),
        FormatTreeNode::new(format!(
            "filters: [{}]",
            op.push_down_predicates.as_ref().map_or_else(
                || "".to_string(),
                |predicates| { predicates.iter().map(format_scalar).join(", ") },
            ),
        )),
        FormatTreeNode::new(format!(
            "order by: [{}]",
            op.order_by.as_ref().map_or_else(
                || "".to_string(),
                |items| items
                    .iter()
                    .map(|item| format!(
                        "{} (#{}) {}",
                        id_humanizer.humanize_column_id(item.index),
                        item.index,
                        if item.asc { "ASC" } else { "DESC" }
                    ))
                    .collect::<Vec<String>>()
                    .join(", "),
            ),
        )),
        FormatTreeNode::new(format!(
            "limit: {}",
            op.limit.map_or("NONE".to_string(), |l| l.to_string())
        )),
    ])
}

fn format_join(op: &Join) -> String {
    let join_type = match op.join_type {
        JoinType::Cross => "Cross".to_string(),
        JoinType::Inner => "Inner".to_string(),
        JoinType::Left => "Left".to_string(),
        JoinType::Right => "Right".to_string(),
        JoinType::Full => "Full".to_string(),
        JoinType::LeftSemi => "LeftSemi".to_string(),
        JoinType::RightSemi => "RightSemi".to_string(),
        JoinType::LeftAnti => "LeftAnti".to_string(),
        JoinType::RightAnti => "RightAnti".to_string(),
        JoinType::LeftMark => "LeftMark".to_string(),
        JoinType::RightMark => "RightMark".to_string(),
        JoinType::LeftSingle => "LeftSingle".to_string(),
        JoinType::RightSingle => "RightSingle".to_string(),
        JoinType::Asof => "Asof".to_string(),
        JoinType::LeftAsof => "LeftAsof".to_string(),
        JoinType::RightAsof => "RightAsof".to_string(),
    };

    format!("Join({})", join_type)
}

fn join_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &Join,
) -> FormatTreeNode {
    let build_keys = op
        .equi_conditions
        .iter()
        .map(|condition| format_scalar(&condition.right))
        .collect::<Vec<String>>()
        .join(", ");
    let probe_keys = op
        .equi_conditions
        .iter()
        .map(|condition| format_scalar(&condition.left))
        .collect::<Vec<String>>()
        .join(", ");
    let join_filters = op
        .non_equi_conditions
        .iter()
        .map(format_scalar)
        .collect::<Vec<String>>()
        .join(", ");

    FormatTreeNode::with_children(format_join(op), vec![
        FormatTreeNode::new(format!("build keys: [{}]", build_keys)),
        FormatTreeNode::new(format!("probe keys: [{}]", probe_keys)),
        FormatTreeNode::new(format!("other filters: [{}]", join_filters)),
    ])
}

fn aggregate_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &Aggregate,
) -> FormatTreeNode {
    let group_items = op
        .group_items
        .iter()
        .map(|item| format_scalar(&item.scalar))
        .collect::<Vec<String>>()
        .join(", ");
    let agg_funcs = op
        .aggregate_functions
        .iter()
        .map(|item| format!("{} (#{})", format_scalar(&item.scalar), item.index))
        .collect::<Vec<String>>()
        .join(", ");
    FormatTreeNode::with_children(format_aggregate(op), vec![
        FormatTreeNode::new(format!("group items: [{}]", group_items)),
        FormatTreeNode::new(format!("aggregate functions: [{}]", agg_funcs)),
    ])
}

fn window_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &Window,
) -> FormatTreeNode {
    let partition_by_items = op
        .partition_by
        .iter()
        .map(|item| format_scalar(&item.scalar))
        .collect::<Vec<String>>()
        .join(", ");

    let order_by_items = op
        .order_by
        .iter()
        .map(|item| format_scalar(&item.order_by_item.scalar))
        .collect::<Vec<_>>()
        .join(", ");

    let frame = op.frame.to_string();

    FormatTreeNode::with_children("Window".to_string(), vec![
        FormatTreeNode::new(format!("aggregate function: {}", op.function.func_name())),
        FormatTreeNode::new(format!("partition items: [{}]", partition_by_items)),
        FormatTreeNode::new(format!("order by items: [{}]", order_by_items)),
        FormatTreeNode::new(format!("frame: [{}]", frame)),
    ])
}

fn filter_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &Filter,
) -> FormatTreeNode {
    let scalars = op
        .predicates
        .iter()
        .map(format_scalar)
        .collect::<Vec<String>>()
        .join(", ");
    FormatTreeNode::with_children("Filter".to_string(), vec![FormatTreeNode::new(format!(
        "filters: [{}]",
        scalars
    ))])
}

fn eval_scalar_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &EvalScalar,
) -> FormatTreeNode {
    let scalars = op
        .items
        .iter()
        .sorted_by(|a, b| a.index.cmp(&b.index))
        .map(|item| format!("{} AS (#{})", format_scalar(&item.scalar), item.index))
        .collect::<Vec<String>>()
        .join(", ");
    FormatTreeNode::with_children("EvalScalar".to_string(), vec![FormatTreeNode::new(
        format!("scalars: [{}]", scalars),
    )])
}

fn sort_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    id_humanizer: &I,
    op: &Sort,
) -> FormatTreeNode {
    let scalars = op
        .items
        .iter()
        .map(|item| {
            format!(
                "{} (#{}) {}",
                id_humanizer.humanize_column_id(item.index),
                item.index,
                if item.asc { "ASC" } else { "DESC" }
            )
        })
        .collect::<Vec<String>>()
        .join(", ");
    let limit = op.limit.map_or("NONE".to_string(), |l| l.to_string());

    FormatTreeNode::with_children("Sort".to_string(), vec![
        FormatTreeNode::new(format!("sort keys: [{}]", scalars)),
        FormatTreeNode::new(format!("limit: [{}]", limit)),
    ])
}

fn limit_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &Limit,
) -> FormatTreeNode {
    let limit = op.limit.unwrap_or_default();
    FormatTreeNode::with_children("Limit".to_string(), vec![
        FormatTreeNode::new(format!("limit: [{}]", limit)),
        FormatTreeNode::new(format!("offset: [{}]", op.offset)),
    ])
}

fn exchange_to_format_tree<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>>(
    _id_humanizer: &I,
    op: &Exchange,
) -> FormatTreeNode {
    match op {
        Exchange::Hash(keys) => {
            FormatTreeNode::with_children(format_exchange(op), vec![FormatTreeNode::new(format!(
                "Exchange(Hash): keys: [{}]",
                keys.iter()
                    .map(format_scalar)
                    .collect::<Vec<String>>()
                    .join(", ")
            ))])
        }
        _ => FormatTreeNode::with_children(format_exchange(op), vec![]),
    }
}
