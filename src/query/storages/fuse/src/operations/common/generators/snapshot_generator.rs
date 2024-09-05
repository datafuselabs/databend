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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::common::ConflictResolveContext;

#[async_trait::async_trait]
pub trait SnapshotGenerator {
    /// Convert to `Any`, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    fn set_conflict_resolve_context(&mut self, _ctx: ConflictResolveContext) {}

    async fn fill_default_values(
        &mut self,
        _schema: TableSchema,
        _snapshot: &Option<Arc<TableSnapshot>>,
    ) -> Result<()> {
        Ok(())
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
        prev_table_seq: Option<u64>,
        txn_mgr: TxnManagerRef,
        table_id: u64,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<TableSnapshot> {
        let mut snapshot = self.do_generate_new_snapshot(
            schema,
            cluster_key_meta,
            &previous,
            prev_table_seq,
            table_meta_timestamps,
        )?;
        decorate_snapshot(&mut snapshot, txn_mgr, previous, table_id)?;
        Ok(snapshot)
    }

    fn do_generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: &Option<Arc<TableSnapshot>>,
        prev_table_seq: Option<u64>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<TableSnapshot>;
}

pub fn decorate_snapshot(
    snapshot: &mut TableSnapshot,
    txn_mgr: TxnManagerRef,
    previous: Option<Arc<TableSnapshot>>,
    table_id: u64,
) -> Result<()> {
    let has_pending_transactional_mutations = {
        let guard = txn_mgr.lock();
        // NOTE:
        // When generating a new snapshot for a mutation of table for the first time,
        // there is no buffered table ID inside txn_mgr for this table.
        guard.is_active() && guard.get_table_from_buffer_by_id(table_id).is_some()
    };

    if has_pending_transactional_mutations {
        // Adjust the `prev_snapshot_id` of the newly created snapshot to match the
        // `prev_snapshot_id` of the table when it first appeared in the transaction.
        let previous_of_previous = previous.as_ref().and_then(|prev| prev.prev_snapshot_id);
        snapshot.prev_snapshot_id = previous_of_previous;
    }
    Ok(())
}
