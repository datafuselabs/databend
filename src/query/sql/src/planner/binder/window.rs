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
use std::sync::Arc;

use databend_common_ast::ast::WindowDefinition;
use databend_common_ast::ast::WindowSpec;
use databend_common_ast::Span;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::select::SelectList;
use crate::binder::ColumnBinding;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::LagLeadFunction;
use crate::plans::NthValueFunction;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Sort;
use crate::plans::SortItem;
use crate::plans::SubqueryExpr;
use crate::plans::VisitorMut;
use crate::plans::Window;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::BindContext;
use crate::Binder;
use crate::ColumnEntry;
use crate::DerivedColumn;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

impl Binder {
    pub(super) fn bind_window_function(
        &mut self,
        window_info: &WindowFunctionInfo,
        child: SExpr,
    ) -> Result<SExpr> {
        bind_window_function_info(&self.ctx, window_info, child)
    }

    pub(super) fn analyze_window_definition(
        &self,
        bind_context: &mut BindContext,
        window_list: &Option<Vec<WindowDefinition>>,
    ) -> Result<()> {
        if window_list.is_none() {
            return Ok(());
        }
        let window_list = window_list.as_ref().unwrap();
        let (window_specs, mut resolved_window_specs) =
            self.extract_window_definitions(window_list.as_slice())?;

        let window_definitions = dashmap::DashMap::with_capacity(window_specs.len());

        for (name, spec) in window_specs.iter() {
            let new_spec = Self::rewrite_inherited_window_spec(
                spec,
                &window_specs,
                &mut resolved_window_specs,
            )?;
            window_definitions.insert(name.clone(), new_spec);
        }
        bind_context.window_definitions = window_definitions;
        Ok(())
    }

    fn extract_window_definitions(
        &self,
        window_list: &[WindowDefinition],
    ) -> Result<(HashMap<String, WindowSpec>, HashMap<String, WindowSpec>)> {
        let mut window_specs = HashMap::new();
        let mut resolved_window_specs = HashMap::new();
        for window in window_list {
            window_specs.insert(window.name.name.clone(), window.spec.clone());
            if window.spec.existing_window_name.is_none() {
                resolved_window_specs.insert(window.name.name.clone(), window.spec.clone());
            }
        }
        Ok((window_specs, resolved_window_specs))
    }

    fn rewrite_inherited_window_spec(
        window_spec: &WindowSpec,
        window_list: &HashMap<String, WindowSpec>,
        resolved_window: &mut HashMap<String, WindowSpec>,
    ) -> Result<WindowSpec> {
        if window_spec.existing_window_name.is_some() {
            let referenced_name = window_spec
                .existing_window_name
                .as_ref()
                .unwrap()
                .name
                .clone();
            // check if spec is resolved first, so that we no need to resolve again.
            let referenced_window_spec = {
                resolved_window.get(&referenced_name).unwrap_or(
                    window_list
                        .get(&referenced_name)
                        .ok_or_else(|| ErrorCode::SyntaxException("Referenced window not found"))?,
                )
            }
            .clone();

            let resolved_spec = match referenced_window_spec.existing_window_name.clone() {
                Some(_) => Self::rewrite_inherited_window_spec(
                    &referenced_window_spec,
                    window_list,
                    resolved_window,
                )?,
                None => referenced_window_spec.clone(),
            };

            // add to resolved.
            resolved_window.insert(referenced_name, resolved_spec.clone());

            // check semantic
            if !window_spec.partition_by.is_empty() {
                return Err(ErrorCode::SemanticError(
                    "WINDOW specification with named WINDOW reference cannot specify PARTITION BY",
                ));
            }
            if !window_spec.order_by.is_empty() && !resolved_spec.order_by.is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY",
                ));
            }
            if resolved_spec.window_frame.is_some() {
                return Err(ErrorCode::SemanticError(
                    "Cannot reference named WINDOW containing frame specification",
                ));
            }

            // resolve referenced window
            let mut partition_by = window_spec.partition_by.clone();
            if !resolved_spec.partition_by.is_empty() {
                partition_by = resolved_spec.partition_by.clone();
            }

            let mut order_by = window_spec.order_by.clone();
            if order_by.is_empty() && !resolved_spec.order_by.is_empty() {
                order_by = resolved_spec.order_by.clone();
            }

            let mut window_frame = window_spec.window_frame.clone();
            if window_frame.is_none() && resolved_spec.window_frame.is_some() {
                window_frame = resolved_spec.window_frame;
            }

            // replace with new window spec
            let new_window_spec = WindowSpec {
                existing_window_name: None,
                partition_by,
                order_by,
                window_frame,
            };
            Ok(new_window_spec)
        } else {
            Ok(window_spec.clone())
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct WindowInfo {
    pub window_functions: Vec<WindowFunctionInfo>,
    pub window_functions_map: HashMap<String, usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WindowFunctionInfo {
    pub span: Span,
    pub index: IndexType,
    pub func: WindowFuncType,
    pub arguments: Vec<ScalarItem>,
    pub partition_by_items: Vec<ScalarItem>,
    pub order_by_items: Vec<WindowOrderByInfo>,
    pub frame: WindowFuncFrame,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct WindowOrderByInfo {
    pub order_by_item: ScalarItem,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
}

pub(super) struct WindowRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
}

impl<'a> WindowRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    fn replace_window_function(&mut self, window: &WindowFunc) -> Result<WindowFunc> {
        let mut replaced_partition_items: Vec<ScalarExpr> =
            Vec::with_capacity(window.partition_by.len());
        let mut replaced_order_by_items: Vec<WindowOrderBy> =
            Vec::with_capacity(window.order_by.len());
        let mut window_args = vec![];

        let window_func_name = window.func.func_name();
        let func = match &window.func {
            WindowFuncType::Aggregate(agg) => {
                // resolve aggregate function args in window function.
                let mut replaced_args: Vec<ScalarExpr> = Vec::with_capacity(agg.args.len());
                for (i, arg) in agg.args.iter().enumerate() {
                    let mut arg = arg.clone();
                    let mut aggregate_rewriter = self.as_window_aggregate_rewriter();
                    aggregate_rewriter.visit(&mut arg)?;
                    let name = format!("{window_func_name}_arg_{i}");
                    let replaced_arg = self.replace_expr(&name, &arg)?;
                    window_args.push(ScalarItem {
                        index: replaced_arg.column.index,
                        scalar: arg,
                    });
                    replaced_args.push(replaced_arg.into());
                }
                WindowFuncType::Aggregate(AggregateFunction {
                    display_name: agg.display_name.clone(),
                    func_name: agg.func_name.clone(),
                    distinct: agg.distinct,
                    params: agg.params.clone(),
                    args: replaced_args,
                    return_type: agg.return_type.clone(),
                })
            }
            WindowFuncType::LagLead(ll) => {
                let (new_arg, new_default) =
                    self.replace_lag_lead_args(&mut window_args, &window_func_name, ll)?;

                WindowFuncType::LagLead(LagLeadFunction {
                    is_lag: ll.is_lag,
                    arg: Box::new(new_arg),
                    offset: ll.offset,
                    default: new_default,
                    return_type: ll.return_type.clone(),
                })
            }
            WindowFuncType::NthValue(func) => {
                let mut arg = (*func.arg).clone();
                let mut aggregate_rewriter = self.as_window_aggregate_rewriter();
                aggregate_rewriter.visit(&mut arg)?;
                let name = format!("{window_func_name}_arg");
                let replaced_arg = self.replace_expr(&name, &arg)?;
                window_args.push(ScalarItem {
                    index: replaced_arg.column.index,
                    scalar: arg,
                });
                WindowFuncType::NthValue(NthValueFunction {
                    n: func.n,
                    arg: Box::new(replaced_arg.into()),
                    return_type: func.return_type.clone(),
                    ignore_null: func.ignore_null,
                })
            }
            func => func.clone(),
        };

        // resolve partition by
        let mut partition_by_items = vec![];
        for (i, part) in window.partition_by.iter().enumerate() {
            let mut part = part.clone();
            let mut aggregate_rewriter = self.as_window_aggregate_rewriter();
            aggregate_rewriter.visit(&mut part)?;
            let name = format!("{window_func_name}_part_{i}");
            let replaced_part = self.replace_expr(&name, &part)?;
            partition_by_items.push(ScalarItem {
                index: replaced_part.column.index,
                scalar: part,
            });
            replaced_partition_items.push(replaced_part.into());
        }

        // resolve order by
        let mut order_by_items = vec![];
        for (i, order) in window.order_by.iter().enumerate() {
            let mut order_expr = order.expr.clone();
            let mut aggregate_rewriter = self.as_window_aggregate_rewriter();
            aggregate_rewriter.visit(&mut order_expr)?;
            let name = format!("{window_func_name}_order_{i}");
            let replaced_order = self.replace_expr(&name, &order_expr)?;
            order_by_items.push(WindowOrderByInfo {
                order_by_item: ScalarItem {
                    index: replaced_order.column.index,
                    scalar: order_expr,
                },
                asc: order.asc,
                nulls_first: order.nulls_first,
            });
            replaced_order_by_items.push(WindowOrderBy {
                expr: replaced_order.into(),
                asc: order.asc,
                nulls_first: order.nulls_first,
            });
        }

        let index = self.metadata.write().add_derived_column(
            window.display_name.clone(),
            window.func.return_type(),
            Some(ScalarExpr::WindowFunction(window.clone())),
        );

        // create window info
        let window_info = WindowFunctionInfo {
            span: window.span,
            index,
            func: func.clone(),
            arguments: window_args,
            partition_by_items,
            order_by_items,
            frame: window.frame.clone(),
        };

        // push window info to BindContext
        let window_infos = &mut self.bind_context.windows;
        window_infos.window_functions.push(window_info);
        window_infos.window_functions_map.insert(
            window.display_name.clone(),
            window_infos.window_functions.len() - 1,
        );

        let replaced_window = WindowFunc {
            span: window.span,
            display_name: window.display_name.clone(),
            func,
            partition_by: replaced_partition_items,
            order_by: replaced_order_by_items,
            frame: window.frame.clone(),
        };

        Ok(replaced_window)
    }

    fn replace_lag_lead_args(
        &mut self,
        window_args: &mut Vec<ScalarItem>,
        window_func_name: &String,
        f: &LagLeadFunction,
    ) -> Result<(ScalarExpr, Option<Box<ScalarExpr>>)> {
        let mut arg = (*f.arg).clone();
        let mut aggregate_rewriter = self.as_window_aggregate_rewriter();
        aggregate_rewriter.visit(&mut arg)?;
        let name = format!("{window_func_name}_arg");
        let replaced_arg = self.replace_expr(&name, &arg)?;
        window_args.push(ScalarItem {
            scalar: arg,
            index: replaced_arg.column.index,
        });
        let new_default = match &f.default {
            None => None,
            Some(d) => {
                let mut d = (**d).clone();
                let mut aggregate_rewriter = self.as_window_aggregate_rewriter();
                aggregate_rewriter.visit(&mut d)?;
                let name = format!("{window_func_name}_default_value");
                let replaced_default = self.replace_expr(&name, &d)?;
                window_args.push(ScalarItem {
                    scalar: d,
                    index: replaced_default.column.index,
                });
                Some(Box::new(replaced_default.into()))
            }
        };

        Ok((replaced_arg.into(), new_default))
    }

    fn replace_expr(&self, name: &str, arg: &ScalarExpr) -> Result<BoundColumnRef> {
        if let ScalarExpr::BoundColumnRef(col) = &arg {
            Ok(col.clone())
        } else {
            for entry in self.metadata.read().columns() {
                if let ColumnEntry::DerivedColumn(DerivedColumn {
                    scalar_expr,
                    alias,
                    column_index,
                    data_type,
                    ..
                }) = entry
                {
                    if scalar_expr.as_ref() == Some(arg) {
                        // Generate a ColumnBinding for each argument of aggregates
                        let column = ColumnBindingBuilder::new(
                            alias.to_string(),
                            *column_index,
                            Box::new(data_type.clone()),
                            Visibility::Visible,
                        )
                        .build();

                        return Ok(BoundColumnRef {
                            span: arg.span(),
                            column,
                        });
                    }
                }
            }
            let ty = arg.data_type()?;

            let index = self.metadata.write().add_derived_column(
                name.to_string(),
                ty.clone(),
                Some(arg.clone()),
            );

            // Generate a ColumnBinding for each argument of aggregates
            let column = ColumnBindingBuilder::new(
                name.to_string(),
                index,
                Box::new(ty),
                Visibility::Visible,
            )
            .build();
            Ok(BoundColumnRef {
                span: arg.span(),
                column,
            })
        }
    }

    pub fn as_window_aggregate_rewriter(&self) -> WindowAggregateRewriter {
        WindowAggregateRewriter {
            bind_context: self.bind_context,
        }
    }
}

impl<'a> VisitorMut<'a> for WindowRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::WindowFunction(window) = expr {
            *expr = {
                let window_infos = &self.bind_context.windows;

                if let Some(column) =
                    find_replaced_window_function(window_infos, window, &window.display_name)
                {
                    BoundColumnRef { span: None, column }.into()
                } else {
                    self.replace_window_function(window)?.into()
                }
            };
            return Ok(());
        }
        walk_expr_mut(self, expr)
    }
    fn visit_window_function(&mut self, window: &'a mut WindowFunc) -> Result<()> {
        *window = self.replace_window_function(window)?;
        Ok(())
    }

    fn visit_subquery_expr(&mut self, _: &'a mut SubqueryExpr) -> Result<()> {
        // TODO(leiysky): should we recursively process subquery here?
        Ok(())
    }
}

pub struct WindowAggregateRewriter<'a> {
    pub bind_context: &'a BindContext,
}

impl<'a> VisitorMut<'a> for WindowAggregateRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::AggregateFunction(agg_func) = expr {
            if let Some(index) = self
                .bind_context
                .aggregate_info
                .aggregate_functions_map
                .get(&agg_func.display_name)
            {
                let agg = &self.bind_context.aggregate_info.aggregate_functions[*index];
                let column_binding = ColumnBindingBuilder::new(
                    agg_func.display_name.clone(),
                    agg.index,
                    agg_func.return_type.clone(),
                    Visibility::Visible,
                )
                .build();

                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();

                return Ok(());
            } else {
                return Err(ErrorCode::BadArguments("Invalid window function argument"));
            }
        }

        walk_expr_mut(self, expr)
    }
}

/// Replace [`WindowFunction`] with a [`ColumnBinding`] if the function is already replaced.
pub fn find_replaced_window_function(
    window_info: &WindowInfo,
    window: &WindowFunc,
    new_name: &str,
) -> Option<ColumnBinding> {
    window_info
        .window_functions_map
        .get(&window.display_name)
        .map(|i| {
            let window_func_info = &window_info.window_functions[*i];
            debug_assert_eq!(
                window_func_info.func.return_type(),
                window.func.return_type()
            );
            ColumnBindingBuilder::new(
                new_name.to_string(),
                window_func_info.index,
                Box::new(window.func.return_type()),
                Visibility::Visible,
            )
            .build()
        })
}

pub fn bind_window_function_info(
    ctx: &Arc<dyn TableContext>,
    window_info: &WindowFunctionInfo,
    child: SExpr,
) -> Result<SExpr> {
    let window_plan = Window {
        span: window_info.span,
        index: window_info.index,
        function: window_info.func.clone(),
        arguments: window_info.arguments.clone(),
        partition_by: window_info.partition_by_items.clone(),
        order_by: window_info.order_by_items.clone(),
        frame: window_info.frame.clone(),
        limit: None,
    };

    // eval scalars before sort
    // Generate a `EvalScalar` as the input of `Window`.
    let mut scalar_items: Vec<ScalarItem> = Vec::new();
    for arg in &window_plan.arguments {
        scalar_items.push(arg.clone());
    }
    for part in &window_plan.partition_by {
        scalar_items.push(part.clone());
    }
    for order in &window_plan.order_by {
        scalar_items.push(order.order_by_item.clone())
    }

    let child = if !scalar_items.is_empty() {
        let eval_scalar_plan = EvalScalar {
            items: scalar_items,
        };
        SExpr::create_unary(Arc::new(eval_scalar_plan.into()), Arc::new(child))
    } else {
        child
    };

    let default_nulls_first = !ctx
        .get_settings()
        .get_sql_dialect()
        .unwrap()
        .is_null_biggest();

    let mut sort_items: Vec<SortItem> = vec![];
    if !window_plan.partition_by.is_empty() {
        for part in window_plan.partition_by.iter() {
            sort_items.push(SortItem {
                index: part.index,
                asc: true,
                nulls_first: default_nulls_first,
            });
        }
    }

    for order in window_plan.order_by.iter() {
        sort_items.push(SortItem {
            index: order.order_by_item.index,
            asc: order.asc.unwrap_or(true),
            nulls_first: order.nulls_first.unwrap_or(default_nulls_first),
        });
    }

    let child = if !sort_items.is_empty() {
        let sort_plan = Sort {
            items: sort_items,
            limit: window_plan.limit,
            after_exchange: None,
            pre_projection: None,
            window_partition: window_plan.partition_by.clone(),
        };
        SExpr::create_unary(Arc::new(sort_plan.into()), Arc::new(child))
    } else {
        child
    };

    Ok(SExpr::create_unary(
        Arc::new(window_plan.into()),
        Arc::new(child),
    ))
}

impl Binder {
    /// Analyze windows in select clause, this will rewrite window functions.
    /// See [`WindowRewriter`] for more details.
    pub(crate) fn analyze_window(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = WindowRewriter::new(bind_context, self.metadata.clone());
            rewriter.visit(&mut item.scalar)?;
        }

        Ok(())
    }
}
