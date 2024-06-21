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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use super::RelationalProperty;
use crate::optimizer::rule::AppliedRules;
use crate::optimizer::rule::RuleID;
use crate::optimizer::StatInfo;
use crate::plans::Exchange;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::SubqueryExpr;
use crate::plans::UDFCall;
use crate::plans::UDFLambdaCall;
use crate::plans::Visitor;
use crate::plans::WindowFuncType;
use crate::IndexType;
use crate::ScalarExpr;

/// `SExpr` is abbreviation of single expression, which is a tree of relational operators.
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SExpr {
    pub(crate) plan: Arc<RelOperator>,
    pub(crate) children: Vec<Arc<SExpr>>,

    pub(crate) original_group: Option<IndexType>,

    /// A cache of relational property of current `SExpr`, will
    /// be lazily computed as soon as `RelExpr::derive_relational_prop`
    /// is invoked on current `SExpr`.
    ///
    /// Since `SExpr` is `Send + Sync`, we use `Mutex` to protect
    /// the cache.
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub(crate) rel_prop: Arc<Mutex<Option<Arc<RelationalProperty>>>>,

    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub(crate) stat_info: Arc<Mutex<Option<Arc<StatInfo>>>>,

    /// A bitmap to record applied rules on current SExpr, to prevent
    /// redundant transformations.
    pub(crate) applied_rules: AppliedRules,
}

impl SExpr {
    pub fn create(
        plan: Arc<RelOperator>,
        children: impl IntoIterator<Item = Arc<SExpr>>,
        original_group: Option<IndexType>,
        rel_prop: Option<Arc<RelationalProperty>>,
        stat_info: Option<Arc<StatInfo>>,
    ) -> Self {
        SExpr {
            plan,
            children: children.into_iter().collect(),
            original_group,
            rel_prop: Arc::new(Mutex::new(rel_prop)),
            stat_info: Arc::new(Mutex::new(stat_info)),
            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_unary(plan: Arc<RelOperator>, child: Arc<SExpr>) -> Self {
        Self::create(plan, vec![child], None, None, None)
    }

    pub fn create_binary(
        plan: Arc<RelOperator>,
        left_child: Arc<SExpr>,
        right_child: Arc<SExpr>,
    ) -> Self {
        Self::create(plan, vec![left_child, right_child], None, None, None)
    }

    pub fn create_leaf(plan: Arc<RelOperator>) -> Self {
        Self::create(plan, vec![], None, None, None)
    }

    pub fn plan(&self) -> &RelOperator {
        &self.plan
    }

    pub fn children(&self) -> impl Iterator<Item = &SExpr> {
        self.children.iter().map(|v| v.as_ref())
    }

    pub fn child(&self, n: usize) -> Result<&SExpr> {
        self.children
            .get(n)
            .map(|v| v.as_ref())
            .ok_or_else(|| ErrorCode::Internal(format!("Invalid children index: {}", n)))
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.original_group
    }

    /// Replace children with given new `children`.
    /// Note that this method will keep the `applied_rules` of
    /// current `SExpr` unchanged.
    pub fn replace_children(&self, children: impl IntoIterator<Item = Arc<SExpr>>) -> Self {
        Self {
            plan: self.plan.clone(),
            original_group: None,
            rel_prop: Arc::new(Mutex::new(None)),
            stat_info: Arc::new(Mutex::new(None)),
            applied_rules: self.applied_rules.clone(),
            children: children.into_iter().collect(),
        }
    }

    pub fn replace_plan(&self, plan: Arc<RelOperator>) -> Self {
        Self {
            plan,
            original_group: None,
            rel_prop: Arc::new(Mutex::new(None)),
            stat_info: Arc::new(Mutex::new(None)),
            applied_rules: self.applied_rules.clone(),
            children: self.children.clone(),
        }
    }

    /// Record the applied rule id in current SExpr
    pub(crate) fn set_applied_rule(&mut self, rule_id: &RuleID) {
        self.applied_rules.set(rule_id, true);
    }

    /// Check if a rule is applied for current SExpr
    pub(crate) fn applied_rule(&self, rule_id: &RuleID) -> bool {
        self.applied_rules.get(rule_id)
    }

    /// Check if contain subquery
    pub(crate) fn contain_subquery(&self) -> bool {
        if !find_subquery(&self.plan) {
            return self.children.iter().any(|child| child.contain_subquery());
        }
        true
    }

    pub fn get_udfs(&self) -> Result<HashSet<&String>> {
        let mut udfs = HashSet::new();

        match self.plan.as_ref() {
            RelOperator::Scan(scan) => {
                let Scan {
                    push_down_predicates,
                    prewhere,
                    agg_index,
                    ..
                } = scan;

                if let Some(push_down_predicates) = push_down_predicates {
                    for push_down_predicate in push_down_predicates {
                        get_udf_names(push_down_predicate)?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
                if let Some(prewhere) = prewhere {
                    for predicate in &prewhere.predicates {
                        get_udf_names(predicate)?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
                if let Some(agg_index) = agg_index {
                    for predicate in &agg_index.predicates {
                        for udf in get_udf_names(predicate)? {
                            udfs.insert(udf);
                        }
                    }
                    for selection in &agg_index.selection {
                        get_udf_names(&selection.scalar)?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
            }
            RelOperator::Exchange(exchange) => {
                if let Exchange::Hash(hash) = exchange {
                    for hash in hash {
                        for udf in get_udf_names(hash)? {
                            udfs.insert(udf);
                        }
                    }
                }
            }
            RelOperator::Join(op) => {
                for left in &op.left_conditions {
                    get_udf_names(left)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
                for right in &op.right_conditions {
                    get_udf_names(right)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
                for non in &op.non_equi_conditions {
                    get_udf_names(non)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::EvalScalar(op) => {
                for item in &op.items {
                    get_udf_names(&item.scalar)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Filter(op) => {
                for predicate in &op.predicates {
                    get_udf_names(predicate)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Aggregate(op) => {
                for group_items in &op.group_items {
                    get_udf_names(&group_items.scalar)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
                for agg_func in &op.aggregate_functions {
                    get_udf_names(&agg_func.scalar)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Window(op) => {
                for func in &op.function {
                    match func {
                        WindowFuncType::Aggregate(agg) => {
                            for arg in &agg.args {
                                get_udf_names(arg)?.iter().for_each(|udf| {
                                    udfs.insert(*udf);
                                });
                            }
                        }
                        WindowFuncType::LagLead(lag_lead) => {
                            // udfs_pad(&mut udfs, f, &lag_lead.arg)?;
                            get_udf_names(&lag_lead.arg)?.iter().for_each(|udf| {
                                udfs.insert(*udf);
                            });
                            if let Some(default) = &lag_lead.default {
                                get_udf_names(default)?.iter().for_each(|udf| {
                                    udfs.insert(*udf);
                                });
                            }
                        }
                        WindowFuncType::NthValue(nth) => {
                            get_udf_names(&nth.arg)?.iter().for_each(|udf| {
                                udfs.insert(*udf);
                            });
                        }
                        _ => {}
                    }
                }

                for args in &op.arguments {
                    for arg in args {
                        get_udf_names(&arg.scalar)?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
                for order_by in &op.order_by {
                    get_udf_names(&order_by.order_by_item.scalar)?
                        .iter()
                        .for_each(|udf| {
                            udfs.insert(*udf);
                        });
                }
                for partition_by in &op.partition_by {
                    get_udf_names(&partition_by.scalar)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::ProjectSet(op) => {
                for srf in &op.srfs {
                    get_udf_names(&srf.scalar)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Udf(udf) => {
                for item in &udf.items {
                    get_udf_names(&item.scalar)?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Limit(_)
            | RelOperator::UnionAll(_)
            | RelOperator::Sort(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::CteScan(_)
            | RelOperator::AddRowNumber(_)
            | RelOperator::MaterializedCte(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::CacheScan(_)
            | RelOperator::AsyncFunction(_)
            | RelOperator::RecursiveCteScan(_) => {}
        };
        for child in &self.children {
            let udf = child.get_udfs()?;
            udf.iter().for_each(|udf| {
                udfs.insert(*udf);
            })
        }
        Ok(udfs)
    }

    // Add (table_index, column_index) into `Scan` node recursively.
    pub fn add_internal_column_index(
        expr: &SExpr,
        table_index: IndexType,
        column_index: IndexType,
        inverted_index: &Option<InvertedIndexInfo>,
    ) -> SExpr {
        fn add_internal_column_index_into_child(
            s_expr: &SExpr,
            column_index: IndexType,
            table_index: IndexType,
            inverted_index: &Option<InvertedIndexInfo>,
        ) -> SExpr {
            let mut s_expr = s_expr.clone();
            s_expr.plan = if let RelOperator::Scan(mut p) = (*s_expr.plan).clone() {
                if p.table_index == table_index {
                    p.columns.insert(column_index);
                    if inverted_index.is_some() {
                        p.inverted_index = inverted_index.clone();
                    }
                }
                Arc::new(p.into())
            } else {
                s_expr.plan
            };

            if s_expr.children.is_empty() {
                s_expr
            } else {
                let mut children = Vec::with_capacity(s_expr.children.len());
                for child in s_expr.children.iter() {
                    children.push(Arc::new(add_internal_column_index_into_child(
                        child,
                        column_index,
                        table_index,
                        inverted_index,
                    )));
                }

                s_expr.children = children;

                s_expr
            }
        }

        add_internal_column_index_into_child(expr, column_index, table_index, inverted_index)
    }

    // The method will clear the applied rules of current SExpr and its children.
    pub fn clear_applied_rules(&mut self) {
        self.applied_rules.clear();
        let children = self
            .children()
            .map(|child| {
                let mut child = child.clone();
                child.clear_applied_rules();
                Arc::new(child)
            })
            .collect::<Vec<_>>();
        self.children = children;
    }

    pub fn has_merge_exchange(&self) -> bool {
        if let RelOperator::Exchange(Exchange::Merge) = self.plan.as_ref() {
            return true;
        }
        self.children.iter().any(|child| child.has_merge_exchange())
    }
}

fn find_subquery(rel_op: &RelOperator) -> bool {
    match rel_op {
        RelOperator::Scan(_)
        | RelOperator::Limit(_)
        | RelOperator::Exchange(_)
        | RelOperator::UnionAll(_)
        | RelOperator::Sort(_)
        | RelOperator::DummyTableScan(_)
        | RelOperator::CteScan(_)
        | RelOperator::AddRowNumber(_)
        | RelOperator::MaterializedCte(_)
        | RelOperator::ConstantTableScan(_)
        | RelOperator::ExpressionScan(_)
        | RelOperator::CacheScan(_)
        | RelOperator::AsyncFunction(_)
        | RelOperator::RecursiveCteScan(_) => false,
        RelOperator::Join(op) => {
            op.left_conditions.iter().any(find_subquery_in_expr)
                || op.right_conditions.iter().any(find_subquery_in_expr)
                || op.non_equi_conditions.iter().any(find_subquery_in_expr)
        }
        RelOperator::EvalScalar(op) => op
            .items
            .iter()
            .any(|expr| find_subquery_in_expr(&expr.scalar)),
        RelOperator::Filter(op) => op.predicates.iter().any(find_subquery_in_expr),
        RelOperator::Aggregate(op) => {
            op.group_items
                .iter()
                .any(|expr| find_subquery_in_expr(&expr.scalar))
                || op
                    .aggregate_functions
                    .iter()
                    .any(|expr| find_subquery_in_expr(&expr.scalar))
        }
        RelOperator::Window(op) => {
            op.order_by
                .iter()
                .any(|o| find_subquery_in_expr(&o.order_by_item.scalar))
                || op
                    .partition_by
                    .iter()
                    .any(|expr| find_subquery_in_expr(&expr.scalar))
                || op.function.iter().any(|f| match f {
                    WindowFuncType::Aggregate(agg) => agg.args.iter().any(find_subquery_in_expr),
                    _ => false,
                })
        }
        RelOperator::ProjectSet(op) => op
            .srfs
            .iter()
            .any(|expr| find_subquery_in_expr(&expr.scalar)),
        RelOperator::Udf(op) => op
            .items
            .iter()
            .any(|expr| find_subquery_in_expr(&expr.scalar)),
    }
}

fn find_subquery_in_expr(expr: &ScalarExpr) -> bool {
    struct HasSubqueryVisitor {
        has_subquery: bool,
    }

    impl<'a> Visitor<'a> for HasSubqueryVisitor {
        fn visit_subquery(&mut self, _: &'a SubqueryExpr) -> Result<()> {
            self.has_subquery = true;
            Ok(())
        }
    }

    let mut has_subquery = HasSubqueryVisitor {
        has_subquery: false,
    };
    has_subquery.visit(expr).unwrap();
    has_subquery.has_subquery
}

pub fn get_udf_names(scalar: &ScalarExpr) -> Result<HashSet<&String>> {
    struct FindUdfNamesVisitor<'a> {
        udfs: HashSet<&'a String>,
    }

    impl<'a> Visitor<'a> for FindUdfNamesVisitor<'a> {
        fn visit_udf_call(&mut self, udf: &'a UDFCall) -> Result<()> {
            for expr in &udf.arguments {
                self.visit(expr)?;
            }

            self.udfs.insert(&udf.name);
            Ok(())
        }

        fn visit_udf_lambda_call(&mut self, udf: &'a UDFLambdaCall) -> Result<()> {
            self.visit(&udf.scalar)?;
            self.udfs.insert(&udf.func_name);
            Ok(())
        }
    }

    let mut find_udfs = FindUdfNamesVisitor {
        udfs: HashSet::new(),
    };
    find_udfs.visit(scalar)?;
    Ok(find_udfs.udfs)
}
