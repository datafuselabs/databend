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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression as ex;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::NonEmptyString;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::TableCopiedFileInfo {
    type PB = pb::TableCopiedFileInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableCopiedFileInfo) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            etag: p.etag,
            content_length: p.content_length,
            last_modified: match p.last_modified {
                None => None,
                Some(last_modified) => Some(DateTime::<Utc>::from_pb(last_modified)?),
            },
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableCopiedFileInfo, Incompatible> {
        let p = pb::TableCopiedFileInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            etag: self.etag.clone(),
            content_length: self.content_length,
            last_modified: match self.last_modified {
                None => None,
                Some(last_modified) => Some(last_modified.to_pb()?),
            },
        };
        Ok(p)
    }
}

impl FromToProto for mt::EmptyProto {
    type PB = pb::EmptyProto;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::EmptyProto) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {};
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::EmptyProto, Incompatible> {
        let p = pb::EmptyProto {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableNameIdent {
    type PB = pb::TableNameIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableNameIdent) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let non_empty = NonEmptyString::new(p.tenant.clone())
            .map_err(|_e| Incompatible::new("tenant is empty"))?;

        let tenant = Tenant::new_nonempty(non_empty);

        let v = Self {
            tenant,
            db_name: p.db_name,
            table_name: p.table_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableNameIdent, Incompatible> {
        let p = pb::TableNameIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tenant: self.tenant.name().to_string(),
            db_name: self.db_name.clone(),
            table_name: self.table_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::DBIdTableName {
    type PB = pb::DbIdTableName;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DbIdTableName) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            db_id: p.db_id,
            table_name: p.table_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DbIdTableName, Incompatible> {
        let p = pb::DbIdTableName {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            db_id: self.db_id,
            table_name: self.table_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableIdent {
    type PB = pb::TableIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableIdent) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            table_id: p.table_id,
            seq: p.seq,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdent, Incompatible> {
        let p = pb::TableIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            table_id: self.table_id,
            seq: self.seq,
        };

        Ok(p)
    }
}

impl FromToProto for mt::TableMeta {
    type PB = pb::TableMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let schema = p.schema.ok_or_else(|| Incompatible {
            reason: "TableMeta.schema can not be None".to_string(),
        })?;

        let catalog = if p.catalog.is_empty() {
            "default".to_string()
        } else {
            p.catalog
        };
        let mut indexes = BTreeMap::new();
        for (name, index) in p.indexes {
            indexes.insert(name, mt::TableIndex::from_pb(index)?);
        }

        let v = Self {
            schema: Arc::new(ex::TableSchema::from_pb(schema)?),
            catalog,
            engine: p.engine,
            engine_options: p.engine_options,
            storage_params: match p.storage_params {
                Some(sp) => Some(StorageParams::from_pb(sp)?),
                None => None,
            },
            part_prefix: p.part_prefix.unwrap_or("".to_string()),
            options: p.options,
            default_cluster_key: p.default_cluster_key,
            cluster_keys: p.cluster_keys,
            default_cluster_key_id: p.default_cluster_key_id,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            drop_on: match p.drop_on {
                Some(drop_on) => Some(DateTime::<Utc>::from_pb(drop_on)?),
                None => None,
            },
            comment: p.comment,
            field_comments: p.field_comments,
            statistics: p
                .statistics
                .map(mt::TableStatistics::from_pb)
                .transpose()?
                .unwrap_or_default(),
            shared_by: BTreeSet::from_iter(p.shared_by),
            column_mask_policy: if p.column_mask_policy.is_empty() {
                None
            } else {
                Some(p.column_mask_policy)
            },
            indexes,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableMeta, Incompatible> {
        let mut indexes = BTreeMap::new();
        for (name, index) in &self.indexes {
            indexes.insert(name.clone(), index.to_pb()?);
        }
        let p = pb::TableMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            catalog: self.catalog.clone(),
            schema: Some(self.schema.to_pb()?),
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            storage_params: match self.storage_params.clone() {
                Some(sp) => Some(sp.to_pb()?),
                None => None,
            },
            part_prefix: if self.part_prefix.is_empty() {
                None
            } else {
                Some(self.part_prefix.clone())
            },
            options: self.options.clone(),
            default_cluster_key: self.default_cluster_key.clone(),
            cluster_keys: self.cluster_keys.clone(),
            default_cluster_key_id: self.default_cluster_key_id,
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            drop_on: match self.drop_on {
                Some(drop_on) => Some(drop_on.to_pb()?),
                None => None,
            },
            comment: self.comment.clone(),
            field_comments: self.field_comments.clone(),
            statistics: Some(self.statistics.to_pb()?),
            shared_by: Vec::from_iter(self.shared_by.clone()),
            column_mask_policy: self.column_mask_policy.clone().unwrap_or_default(),
            indexes,
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableStatistics {
    type PB = pb::TableStatistics;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableStatistics) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            number_of_rows: p.number_of_rows,
            data_bytes: p.data_bytes,
            compressed_data_bytes: p.compressed_data_bytes,
            index_data_bytes: p.index_data_bytes,
            number_of_segments: p.number_of_segments,
            number_of_blocks: p.number_of_blocks,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableStatistics, Incompatible> {
        let p = pb::TableStatistics {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            number_of_rows: self.number_of_rows,
            data_bytes: self.data_bytes,
            compressed_data_bytes: self.compressed_data_bytes,
            index_data_bytes: self.index_data_bytes,
            number_of_segments: self.number_of_segments,
            number_of_blocks: self.number_of_blocks,
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableIdList {
    type PB = pb::TableIdList;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableIdList) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdList, Incompatible> {
        let p = pb::TableIdList {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableIndex {
    type PB = pb::TableIndex;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableIndex) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            name: p.name,
            column_ids: p.column_ids,
            sync_creation: p.sync_creation,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIndex, Incompatible> {
        let p = pb::TableIndex {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            column_ids: self.column_ids.clone(),
            sync_creation: self.sync_creation,
        };
        Ok(p)
    }
}
