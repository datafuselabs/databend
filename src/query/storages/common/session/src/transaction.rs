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

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MatchSeq;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table_id_ranges::is_temp_table_id;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;

use crate::temp_table::TempTable;

#[derive(Debug, Clone)]
pub struct TxnManager {
    state: TxnState,
    txn_buffer: TxnBuffer,
    txn_id: String,
}

pub type TxnManagerRef = Arc<Mutex<TxnManager>>;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum TxnState {
    AutoCommit,
    Active,
    Fail,
}

#[derive(Debug, Clone, Default)]
pub struct TxnBuffer {
    table_desc_to_id: HashMap<String, u64>,
    mutated_tables: HashMap<u64, TableInfo>,
    copied_files: HashMap<u64, Vec<UpsertTableCopiedFileReq>>,
    update_stream_meta: HashMap<u64, UpdateStreamMetaReq>,
    deduplicated_labels: HashSet<String>,
    stream_tables: HashMap<u64, StreamSnapshot>,
    need_purge_files: Vec<(StageInfo, Vec<String>)>,

    pub table_meta_timestamps: HashMap<u64, TableMetaTimestamps>,

    temp_table_desc_to_id: HashMap<String, u64>,
    mutated_temp_tables: HashMap<u64, TempTable>,
}

#[derive(Debug, Clone)]
pub struct StreamSnapshot {
    pub stream: TableInfo,
    pub source: TableInfo,
    pub max_batch_size: Option<u64>,
}

impl TxnBuffer {
    fn clear(&mut self) {
        std::mem::take(self);
    }

    fn update_multi_table_meta(&mut self, mut req: UpdateMultiTableMetaReq) {
        for (req, table_info) in req.update_table_metas {
            let table_id = req.table_id;
            self.table_desc_to_id
                .insert(table_info.desc.clone(), table_id);

            self.mutated_tables.insert(table_id, TableInfo {
                meta: req.new_table_meta.clone(),
                ..table_info.clone()
            });
        }

        for (table_id, file) in std::mem::take(&mut req.copied_files) {
            self.copied_files.entry(table_id).or_default().push(file);
        }

        self.update_stream_metas(&req.update_stream_metas);

        self.deduplicated_labels.extend(req.deduplicated_labels);

        for req in req.update_temp_tables {
            let (db_name, table_name) = req.desc.split_once('.').unwrap();
            self.temp_table_desc_to_id
                .insert(req.desc.clone(), req.table_id);
            self.mutated_temp_tables.insert(req.table_id, TempTable {
                db_name: db_name.to_string(),
                table_name: table_name.to_string(),
                meta: req.new_table_meta.clone(),
                copied_files: req.copied_files.clone(),
            });
        }
    }

    fn update_stream_metas(&mut self, reqs: &[UpdateStreamMetaReq]) {
        for stream_meta in reqs.iter() {
            self.update_stream_meta
                .entry(stream_meta.stream_id)
                .or_insert(stream_meta.clone());
        }
    }
}

impl TxnManager {
    pub fn init() -> TxnManagerRef {
        Arc::new(Mutex::new(TxnManager {
            state: TxnState::AutoCommit,
            txn_buffer: TxnBuffer::default(),
            txn_id: "".to_string(),
        }))
    }

    pub fn begin(&mut self) {
        if let TxnState::AutoCommit = self.state {
            self.txn_id = uuid::Uuid::new_v4().to_string();
            self.state = TxnState::Active
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.txn_id
    }

    pub fn clear(&mut self) {
        self.state = TxnState::AutoCommit;
        self.txn_buffer.clear();
        self.txn_id = "".to_string();
    }

    pub fn set_fail(&mut self) {
        if let TxnState::Active = self.state {
            self.state = TxnState::Fail;
        }
    }

    pub fn set_auto_commit(&mut self) {
        self.state = TxnState::AutoCommit;
    }

    pub fn force_set_fail(&mut self) {
        self.state = TxnState::Fail;
    }

    pub fn is_fail(&self) -> bool {
        matches!(self.state, TxnState::Fail)
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TxnState::Active)
    }

    pub fn state(&self) -> TxnState {
        self.state.clone()
    }

    pub fn update_multi_table_meta(&mut self, req: UpdateMultiTableMetaReq) {
        self.txn_buffer.update_multi_table_meta(req);
    }

    pub fn update_stream_metas(&mut self, reqs: &[UpdateStreamMetaReq]) {
        self.txn_buffer.update_stream_metas(reqs);
    }

    // for caching stream table to impl the rr semantics
    pub fn upsert_stream_table(
        &mut self,
        stream: TableInfo,
        source: TableInfo,
        max_batch_size: Option<u64>,
    ) {
        self.txn_buffer
            .stream_tables
            .insert(stream.ident.table_id, StreamSnapshot {
                stream,
                source,
                max_batch_size,
            });
    }

    pub fn upsert_table_desc_to_id(&mut self, table: TableInfo) {
        self.txn_buffer
            .table_desc_to_id
            .insert(table.desc.clone(), table.ident.table_id);
    }

    pub fn get_stream_table(&self, stream_desc: &str) -> Option<StreamSnapshot> {
        self.txn_buffer
            .table_desc_to_id
            .get(stream_desc)
            .and_then(|id| self.txn_buffer.stream_tables.get(id))
            .cloned()
    }

    pub fn get_table_from_buffer(
        &self,
        _tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Option<TableInfo> {
        let desc = format!("'{}'.'{}'", db_name, table_name);
        let temp_table_id = self.txn_buffer.temp_table_desc_to_id.get(&desc);
        if let Some(id) = temp_table_id {
            let table = self.txn_buffer.mutated_temp_tables.get(id).unwrap();
            return Some(TableInfo::new(
                &table.db_name,
                &table.table_name,
                TableIdent {
                    table_id: *id,
                    seq: 0,
                },
                table.meta.clone(),
            ));
        }

        self.txn_buffer
            .table_desc_to_id
            .get(&desc)
            .and_then(|id| self.txn_buffer.mutated_tables.get(id))
            .cloned()
    }

    pub fn get_table_from_buffer_by_id(&self, table_id: u64) -> Option<TableInfo> {
        self.txn_buffer
            .mutated_temp_tables
            .get(&table_id)
            .map(|t| {
                TableInfo::new(
                    &t.db_name,
                    &t.table_name,
                    TableIdent { table_id, seq: 0 },
                    t.meta.clone(),
                )
            })
            .or_else(|| {
                self.txn_buffer
                    .mutated_tables
                    .get(&table_id)
                    .cloned()
                    .or_else(|| {
                        self.txn_buffer
                            .stream_tables
                            .get(&table_id)
                            .map(|snapshot| snapshot.stream.clone())
                    })
            })
    }

    pub fn req(&self) -> UpdateMultiTableMetaReq {
        let mut copied_files = Vec::new();
        for (tbl_id, v) in &self.txn_buffer.copied_files {
            for file in v {
                copied_files.push((*tbl_id, file.clone()));
            }
        }
        UpdateMultiTableMetaReq {
            update_table_metas: self
                .txn_buffer
                .mutated_tables
                .iter()
                .map(|(id, info)| {
                    (
                        UpdateTableMetaReq {
                            table_id: *id,
                            seq: MatchSeq::Exact(info.ident.seq),
                            new_table_meta: info.meta.clone(),
                        },
                        info.clone(),
                    )
                })
                .collect(),
            copied_files,
            update_stream_metas: self
                .txn_buffer
                .update_stream_meta
                .values()
                .cloned()
                .collect(),
            deduplicated_labels: self
                .txn_buffer
                .deduplicated_labels
                .iter()
                .cloned()
                .collect(),
            update_temp_tables: self
                .txn_buffer
                .mutated_temp_tables
                .iter()
                .map(|(id, t)| UpdateTempTableReq {
                    table_id: *id,
                    new_table_meta: t.meta.clone(),
                    copied_files: t.copied_files.clone(),
                    desc: format!("'{}'.'{}'", t.db_name, t.table_name),
                })
                .collect(),
        }
    }

    pub fn contains_deduplicated_label(&self, label: &str) -> bool {
        self.txn_buffer.deduplicated_labels.contains(label)
    }

    pub fn get_table_copied_file_info(
        &self,
        table_id: u64,
    ) -> BTreeMap<String, TableCopiedFileInfo> {
        let mut ret = BTreeMap::new();
        if !self.is_active() {
            return ret;
        }
        if is_temp_table_id(table_id) {
            let temp_table = self.txn_buffer.mutated_temp_tables.get(&table_id);
            if let Some(temp_table) = temp_table {
                ret.extend(temp_table.copied_files.clone());
            }
        } else {
            let reqs = self.txn_buffer.copied_files.get(&table_id);
            if let Some(reqs) = reqs {
                for req in reqs {
                    ret.extend(req.file_info.clone().into_iter());
                }
            }
        }
        ret
    }

    pub fn add_need_purge_files(&mut self, stage_info: StageInfo, files: Vec<String>) {
        self.txn_buffer.need_purge_files.push((stage_info, files));
    }

    pub fn need_purge_files(&mut self) -> Vec<(StageInfo, Vec<String>)> {
        std::mem::take(&mut self.txn_buffer.need_purge_files)
    }

    pub fn get_table_meta_timestamps(
        &mut self,
        table_id: u64,
        previous_snapshot: Option<Arc<TableSnapshot>>,
        data_retention_time_in_days: i64,
    ) -> TableMetaTimestamps {
        if !self.is_active() {
            return TableMetaTimestamps::new(previous_snapshot, data_retention_time_in_days);
        }

        let entry = self.txn_buffer.table_meta_timestamps.entry(table_id);
        match entry {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let timestamps =
                    TableMetaTimestamps::new(previous_snapshot, data_retention_time_in_days);
                e.insert(timestamps);
                timestamps
            }
        }
    }
}
