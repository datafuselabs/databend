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

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::share as mt;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::NonEmptyString;
use databend_common_protos::pb;
use enumflags2::BitFlags;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::ObjectSharedByShareIds {
    type PB = pb::ObjectSharedByShareIds;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ObjectSharedByShareIds) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            share_ids: BTreeSet::from_iter(p.share_ids.iter().copied()),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ObjectSharedByShareIds, Incompatible> {
        let p = pb::ObjectSharedByShareIds {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            share_ids: Vec::from_iter(self.share_ids.iter().copied()),
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareNameIdent {
    type PB = pb::ShareNameIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareNameIdent) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let non_empty = NonEmptyString::new(p.tenant.clone())
            .map_err(|_e| Incompatible::new("tenant is empty"))?;

        let tenant = Tenant::new_nonempty(non_empty);

        let v = Self {
            tenant,
            share_name: p.share_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ShareNameIdent, Incompatible> {
        let p = pb::ShareNameIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tenant: self.tenant.name().to_string(),
            share_name: self.share_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareGrantObject {
    type PB = pb::ShareGrantObject;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareGrantObject) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match p.object {
            Some(pb::share_grant_object::Object::DbId(db_id)) => {
                Ok(mt::ShareGrantObject::Database(db_id))
            }
            Some(pb::share_grant_object::Object::TableId(table_id)) => {
                Ok(mt::ShareGrantObject::Table(table_id))
            }
            None => Err(Incompatible {
                reason: "ShareGrantObject cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::ShareGrantObject, Incompatible> {
        let object = match self {
            mt::ShareGrantObject::Database(db_id) => {
                Some(pb::share_grant_object::Object::DbId(*db_id))
            }
            mt::ShareGrantObject::Table(table_id) => {
                Some(pb::share_grant_object::Object::TableId(*table_id))
            }
        };

        let p = pb::ShareGrantObject {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object,
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareGrantEntry {
    type PB = pb::ShareGrantEntry;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareGrantEntry) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        // Before https://github.com/datafuselabs/databend/releases/tag/v1.2.321-nightly
        // use from_bits deserialize privilege type, that maybe cause forward compat error.
        // Because old query may not contain new query's privilege type, so from_bits will return err, cause from_pb err.
        // https://docs.rs/enumflags2/0.7.7/enumflags2/struct.BitFlags.html#method.from_bits
        // https://docs.rs/enumflags2/0.7.7/enumflags2/struct.BitFlags.html#method.from_bits_truncate
        let privileges =
            BitFlags::<mt::ShareGrantObjectPrivilege, u64>::from_bits_truncate(p.privileges);
        Ok(mt::ShareGrantEntry {
            object: mt::ShareGrantObject::from_pb(p.object.ok_or_else(|| Incompatible {
                reason: "ShareGrantEntry.object can not be None".to_string(),
            })?)?,
            privileges,
            grant_on: DateTime::<Utc>::from_pb(p.grant_on)?,
            update_on: match p.update_on {
                Some(t) => Some(DateTime::<Utc>::from_pb(t)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<pb::ShareGrantEntry, Incompatible> {
        Ok(pb::ShareGrantEntry {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object: Some(self.object().to_pb()?),
            privileges: self.privileges().bits(),
            grant_on: self.grant_on.to_pb()?,
            update_on: match &self.update_on {
                Some(t) => Some(t.to_pb()?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::ShareMeta {
    type PB = pb::ShareMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareMeta) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let mut entries = BTreeMap::new();
        for entry in p.entries {
            let entry = mt::ShareGrantEntry::from_pb(entry)?;
            entries.insert(entry.to_string(), entry.clone());
        }
        Ok(mt::ShareMeta {
            database: match p.database {
                Some(db) => Some(mt::ShareGrantEntry::from_pb(db)?),
                None => None,
            },
            entries,
            comment: p.comment.clone(),
            accounts: BTreeSet::from_iter(p.accounts.clone()),
            share_from_db_ids: BTreeSet::from_iter(p.share_from_db_ids.clone()),
            share_on: DateTime::<Utc>::from_pb(p.share_on)?,
            update_on: match p.update_on {
                Some(t) => Some(DateTime::<Utc>::from_pb(t)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<pb::ShareMeta, Incompatible> {
        let mut entries = Vec::new();
        for entry in self.entries.iter() {
            entries.push(entry.1.to_pb()?);
        }

        Ok(pb::ShareMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            database: match &self.database {
                Some(db) => Some(mt::ShareGrantEntry::to_pb(db)?),
                None => None,
            },
            entries,
            accounts: Vec::from_iter(self.accounts.clone()),
            share_from_db_ids: Vec::from_iter(self.share_from_db_ids.clone()),
            comment: self.comment.clone(),
            share_on: self.share_on.to_pb()?,
            update_on: match &self.update_on {
                Some(t) => Some(t.to_pb()?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::ShareAccountMeta {
    type PB = pb::ShareAccountMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareAccountMeta) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::ShareAccountMeta {
            account: p.account.clone(),
            share_id: p.share_id,
            share_on: DateTime::<Utc>::from_pb(p.share_on)?,
            accept_on: match p.accept_on {
                Some(t) => Some(DateTime::<Utc>::from_pb(t)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<pb::ShareAccountMeta, Incompatible> {
        Ok(pb::ShareAccountMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            account: self.account.clone(),
            share_id: self.share_id,
            share_on: self.share_on.to_pb()?,
            accept_on: match &self.accept_on {
                Some(t) => Some(t.to_pb()?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::ShareEndpointMeta {
    type PB = pb::ShareEndpointMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ShareEndpointMeta) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::ShareEndpointMeta {
            url: p.url.clone(),
            tenant: p.tenant.clone(),
            args: p.args.clone(),
            comment: p.comment.clone(),
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
        })
    }

    fn to_pb(&self) -> Result<pb::ShareEndpointMeta, Incompatible> {
        Ok(pb::ShareEndpointMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            url: self.url.clone(),
            tenant: self.tenant.clone(),
            args: self.args.clone(),
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
        })
    }
}

impl FromToProto for mt::ShareEndpointIdent {
    type PB = pb::ShareEndpointIdent;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ShareEndpointIdent) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::ShareEndpointIdent {
            tenant: p.tenant.clone(),
            endpoint: p.endpoint,
        })
    }

    fn to_pb(&self) -> Result<pb::ShareEndpointIdent, Incompatible> {
        Ok(pb::ShareEndpointIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tenant: self.tenant.clone(),
            endpoint: self.endpoint.clone(),
        })
    }
}
