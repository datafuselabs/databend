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

use std::hash::Hash;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;

use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::IndexType;

#[derive(Clone, Debug)]
pub struct CteScan {
    pub cte_idx: (usize, usize),
    pub fields: Vec<DataField>,
    pub offsets: Vec<IndexType>,
    pub name: String,
    pub stat: Arc<StatInfo>,
}

impl CteScan {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for field in self.fields.iter() {
            used_columns.insert(field.name().parse()?);
        }
        Ok(used_columns)
    }
}

impl PartialEq for CteScan {
    fn eq(&self, other: &Self) -> bool {
        self.cte_idx == other.cte_idx
    }
}

impl Eq for CteScan {}

impl Hash for CteScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cte_idx.hash(state);
    }
}

impl Operator for CteScan {
    fn rel_op(&self) -> RelOp {
        RelOp::CteScan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.used_columns()?,
            outer_columns: ColumnSet::new(),
            used_columns: self.used_columns()?,
            orderings: vec![],
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: self.stat.cardinality,
            statistics: self.stat.statistics.clone(),
        }))
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Err(ErrorCode::Internal(
            "Cannot compute required property for CteScan".to_string(),
        ))
    }
}
