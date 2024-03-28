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

use std::any::type_name;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::ShareHasNoGrantedDatabase;
use databend_common_meta_app::app_error::UnknownDatabase;
use databend_common_meta_app::app_error::UnknownDatabaseId;
use databend_common_meta_app::app_error::UnknownShare;
use databend_common_meta_app::app_error::UnknownShareAccounts;
use databend_common_meta_app::app_error::UnknownShareEndpoint;
use databend_common_meta_app::app_error::UnknownShareEndpointId;
use databend_common_meta_app::app_error::UnknownShareId;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::app_error::VirtualColumnNotFound;
use databend_common_meta_app::app_error::WrongShare;
use databend_common_meta_app::app_error::WrongShareObject;
use databend_common_meta_app::primitive::Id;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseNameIdent;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::IndexId;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::schema::VirtualColumnNameIdent;
use databend_common_meta_app::share::*;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::txn_condition::Target;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::InvalidArgument;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnGetResponse;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnRequest;
use databend_common_proto_conv::FromToProto;
use enumflags2::BitFlags;
use log::debug;
use log::warn;
use ConditionResult::Eq;

use crate::kv_app_error::KVAppError;
use crate::reply::txn_reply_to_api_result;

pub const DEFAULT_MGET_SIZE: usize = 256;

/// Get value that its type is `u64`.
///
/// It expects the kv-value's type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`,
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`, or
/// `__fd_index/<tenant>/<index_name> -> (seq, index_id)`.
///
/// It returns (seq, `u64` value).
/// If not found, (0,0) is returned.
pub async fn get_u64_value<T: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &T,
) -> Result<(u64, u64), MetaError> {
    let res = kv_api.get_kv(&key.to_string_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, *deserialize_u64(&seq_v.data)?))
    } else {
        Ok((0, 0))
    }
}

#[allow(clippy::type_complexity)]
pub fn deserialize_struct_get_response<K>(
    resp: TxnGetResponse,
) -> Result<(K, Option<SeqV<K::ValueType>>), MetaError>
where
    K: kvapi::Key,
    K::ValueType: FromToProto,
{
    let key = K::from_str_key(&resp.key).map_err(|e| {
        let inv = InvalidReply::new(
            format!("fail to parse {} key, {}", type_name::<K>(), resp.key),
            &e,
        );
        MetaNetworkError::InvalidReply(inv)
    })?;

    if let Some(pb_seqv) = resp.value {
        let seqv = SeqV::from(pb_seqv);
        let value = deserialize_struct::<K::ValueType>(&seqv.data)?;
        let seqv = SeqV::with_meta(seqv.seq, seqv.meta, value);
        Ok((key, Some(seqv)))
    } else {
        Ok((key, None))
    }
}

pub fn deserialize_id_get_response<K>(
    resp: TxnGetResponse,
) -> Result<(K, Option<SeqV<Id>>), MetaError>
where K: kvapi::Key {
    let key = K::from_str_key(&resp.key).map_err(|e| {
        let inv = InvalidReply::new(
            format!("fail to parse {} key, {}", type_name::<K>(), resp.key),
            &e,
        );
        MetaNetworkError::InvalidReply(inv)
    })?;

    if let Some(pb_seqv) = resp.value {
        let seqv = SeqV::from(pb_seqv);
        let id = deserialize_u64(&seqv.data)?;
        let seqv = SeqV::with_meta(seqv.seq, seqv.meta, id);
        Ok((key, Some(seqv)))
    } else {
        Ok((key, None))
    }
}

/// Get value that are encoded with FromToProto.
///
/// It returns seq number and the data.
pub async fn get_pb_value<K>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    k: &K,
) -> Result<(u64, Option<K::ValueType>), MetaError>
where
    K: kvapi::Key,
    K::ValueType: FromToProto,
{
    let res = kv_api.get_kv(&k.to_string_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, Some(deserialize_struct(&seq_v.data)?)))
    } else {
        Ok((0, None))
    }
}

/// Batch get values that are encoded with FromToProto.
pub async fn mget_pb_values<T>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    keys: &[String],
) -> Result<Vec<(u64, Option<T>)>, MetaError>
where
    T: FromToProto,
{
    let seq_bytes = kv_api.mget_kv(keys).await?;
    let mut seq_values = Vec::with_capacity(keys.len());
    for seq_v in seq_bytes {
        if let Some(seq_v) = seq_v {
            let seq = seq_v.seq;
            let v = deserialize_struct(&seq_v.data)?;
            seq_values.push((seq, Some(v)))
        } else {
            seq_values.push((0, None));
        }
    }

    Ok(seq_values)
}

/// Return a vec of structured key(such as `DatabaseNameIdent`), such as:
/// all the `db_name` with prefix `__fd_database/<tenant>/`.
pub async fn list_keys<K: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
) -> Result<Vec<K>, MetaError> {
    let res = kv_api.prefix_list_kv(&key.to_string_key()).await?;

    let mut structured_keys = Vec::with_capacity(res.len());

    for (str_key, _seq_id) in res.iter() {
        let struct_key = K::from_str_key(str_key).map_err(|e| {
            let inv = InvalidReply::new("fail to list_keys", &e);
            MetaNetworkError::InvalidReply(inv)
        })?;
        structured_keys.push(struct_key);
    }

    Ok(structured_keys)
}

/// List kvs whose value's type is `u64`.
///
/// It expects the kv-value' type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`,
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`, or
/// `__fd_index/<tenant>/<index_name> -> (seq, index_id)`.
///
/// It returns a vec of structured key(such as DatabaseNameIdent) and a vec of `u64`.
pub async fn list_u64_value<K: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
) -> Result<(Vec<K>, Vec<u64>), MetaError> {
    let res = kv_api.prefix_list_kv(&key.to_string_key()).await?;

    let n = res.len();

    let mut structured_keys = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);

    for (str_key, seqv) in res.iter() {
        let id = *deserialize_u64(&seqv.data)?;
        values.push(id);

        // Parse key
        let struct_key = K::from_str_key(str_key).map_err(|e| {
            let inv = InvalidReply::new("list_u64_value", &e);
            MetaNetworkError::InvalidReply(inv)
        })?;
        structured_keys.push(struct_key);
    }

    Ok((structured_keys, values))
}

pub fn serialize_u64(value: impl Into<Id>) -> Result<Vec<u8>, MetaNetworkError> {
    let v = serde_json::to_vec(&*value.into()).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(v)
}

pub fn deserialize_u64(v: &[u8]) -> Result<Id, MetaNetworkError> {
    let id = serde_json::from_slice(v).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    Ok(Id::new(id))
}

/// Generate an id on metasrv.
///
/// Ids are categorized by generators.
/// Ids may not be consecutive.
pub async fn fetch_id<T: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    generator: T,
) -> Result<u64, KVAppError> {
    let res = kv_api
        .upsert_kv(UpsertKVReq {
            key: generator.to_string_key(),
            seq: MatchSeq::GE(0),
            value: Operation::Update(b"".to_vec()),
            value_meta: None,
        })
        .await?;

    // seq: MatchSeq::Any always succeeds
    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

pub fn serialize_struct<T>(value: &T) -> Result<Vec<u8>, MetaNetworkError>
where T: FromToProto + 'static {
    let p = value.to_pb().map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(buf)
}

pub fn deserialize_struct<T>(buf: &[u8]) -> Result<T, MetaNetworkError>
where T: FromToProto {
    let p: T::PB = prost::Message::decode(buf).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    let v: T = FromToProto::from_pb(p).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;

    Ok(v)
}

pub async fn send_txn(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    txn_req: TxnRequest,
) -> Result<(bool, Vec<TxnOpResponse>), KVAppError> {
    let tx_reply = kv_api.transaction(txn_req).await?;
    let (succ, responses) = txn_reply_to_api_result(tx_reply)?;
    Ok((succ, responses))
}

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_seq(key: &impl kvapi::Key, op: ConditionResult, seq: u64) -> TxnCondition {
    TxnCondition {
        key: key.to_string_key(),
        expected: op as i32,
        target: Some(Target::Seq(seq)),
    }
}

/// Build a txn operation that puts a record.
pub fn txn_op_put(key: &impl kvapi::Key, value: Vec<u8>) -> TxnOp {
    TxnOp::put(key.to_string_key(), value)
}

// TODO: replace it with common_meta_types::with::With
pub fn txn_op_put_with_expire(key: &impl kvapi::Key, value: Vec<u8>, expire_at: u64) -> TxnOp {
    TxnOp::put_with_expire(key.to_string_key(), value, Some(expire_at))
}

/// Build a txn operation that deletes a record.
pub fn txn_op_del(key: &impl kvapi::Key) -> TxnOp {
    TxnOp::delete(key.to_string_key())
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatabase error
pub fn db_has_to_exist(
    seq: u64,
    db_name_ident: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, db_name_ident :? =(db_name_ident); "db does not exist");

        Err(KVAppError::AppError(AppError::UnknownDatabase(
            UnknownDatabase::new(
                &db_name_ident.db_name,
                format!("{}: {}", msg, db_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a db_id to db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatabaseId error
pub fn db_id_has_to_exist(seq: u64, db_id: u64, msg: impl Display) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, db_name_ident :? =(db_id); "db_id does not exist");

        let app_err = AppError::UnknownDatabaseId(UnknownDatabaseId::new(
            db_id,
            format!("{}: {}", msg, db_id),
        ));

        Err(KVAppError::AppError(app_err))
    } else {
        Ok(())
    }
}

/// Return OK if a `table_name_ident->*` exists by checking the seq.
///
/// Otherwise returns [`AppError::UnknownTable`] error
pub fn assert_table_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), AppError> {
    if seq > 0 {
        return Ok(());
    }

    debug!(seq = seq, name_ident :? =(name_ident); "does not exist");

    Err(UnknownTable::new(
        &name_ident.table_name,
        format!("{}: {}", ctx, name_ident),
    ))?
}

/// Return OK if a `table_id->*` exists by checking the seq.
///
/// Otherwise returns [`AppError::UnknownTableId`] error
pub fn assert_table_id_exist(
    seq: u64,
    table_id: &TableId,
    ctx: impl Display,
) -> Result<(), AppError> {
    if seq > 0 {
        return Ok(());
    }

    debug!(seq = seq, table_id :? =(table_id); "does not exist");

    Err(UnknownTableId::new(
        table_id.table_id,
        format!("{}: {}", ctx, table_id),
    ))?
}

/// Get `table_meta_seq` and [`TableMeta`] by [`TableId`],
/// or return [`AppError::UnknownTableId`] error wrapped in a [`KVAppError`] if not found.
pub async fn get_table_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_id: &TableId,
    ctx: impl Display + Copy,
) -> Result<(u64, TableMeta), KVAppError> {
    let (seq, table_meta): (_, Option<TableMeta>) = get_pb_value(kv_api, table_id).await?;
    assert_table_id_exist(seq, table_id, ctx)?;

    let table_meta = table_meta.unwrap();

    debug!(
        ident :% =(table_id),
        table_meta :? =(&table_meta);
        "{}",
        ctx
    );

    Ok((seq, table_meta))
}

// Return (share_endpoint_id_seq, share_endpoint_id, share_endpoint_meta_seq, share_endpoint_meta)
pub async fn get_share_endpoint_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ShareEndpointIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, ShareEndpointMeta), KVAppError> {
    let (share_endpoint_id_seq, share_endpoint_id) = get_u64_value(kv_api, name_key).await?;
    share_endpoint_has_to_exist(share_endpoint_id_seq, name_key, &msg)?;

    let (share_endpoint_meta_seq, share_endpoint_meta) =
        get_share_endpoint_meta_by_id_or_err(kv_api, share_endpoint_id, msg).await?;

    Ok((
        share_endpoint_id_seq,
        share_endpoint_id,
        share_endpoint_meta_seq,
        share_endpoint_meta,
    ))
}

async fn get_share_endpoint_meta_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_endpoint_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareEndpointMeta), KVAppError> {
    let id_key = ShareEndpointId { share_endpoint_id };

    let (share_endpoint_meta_seq, share_endpoint_meta) = get_pb_value(kv_api, &id_key).await?;
    share_endpoint_meta_has_to_exist(share_endpoint_meta_seq, share_endpoint_id, msg)?;

    Ok((share_endpoint_meta_seq, share_endpoint_meta.unwrap()))
}

fn share_endpoint_meta_has_to_exist(
    seq: u64,
    share_endpoint_id: u64,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(
            seq = seq,
            share_endpoint_id = share_endpoint_id;
            "share endpoint meta does not exist"
        );

        Err(KVAppError::AppError(AppError::UnknownShareEndpointId(
            UnknownShareEndpointId::new(
                share_endpoint_id,
                format!("{}: {}", msg, share_endpoint_id),
            ),
        )))
    } else {
        Ok(())
    }
}

fn share_endpoint_has_to_exist(
    seq: u64,
    name_key: &ShareEndpointIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, name_key :? =(name_key); "share endpoint does not exist");

        Err(KVAppError::AppError(AppError::UnknownShareEndpoint(
            UnknownShareEndpoint::new(&name_key.endpoint, format!("{}: {}", msg, name_key)),
        )))
    } else {
        Ok(())
    }
}

pub async fn get_share_endpoint_id_to_name_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_endpoint_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareEndpointIdent), KVAppError> {
    let id_key = ShareEndpointIdToName { share_endpoint_id };

    let (share_endpoint_name_seq, share_endpoint) = get_pb_value(kv_api, &id_key).await?;
    if share_endpoint_name_seq == 0 {
        debug!(
            share_endpoint_name_seq = share_endpoint_name_seq,
            share_endpoint_id = share_endpoint_id;
            "share meta does not exist"
        );

        return Err(KVAppError::AppError(AppError::UnknownShareEndpointId(
            UnknownShareEndpointId::new(
                share_endpoint_id,
                format!("{}: {}", msg, share_endpoint_id),
            ),
        )));
    }

    Ok((share_endpoint_name_seq, share_endpoint.unwrap()))
}

// Return (share_id_seq, share_id, share_meta_seq, share_meta)
pub async fn get_share_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ShareNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, ShareMeta), KVAppError> {
    let (share_id_seq, share_id) = get_u64_value(kv_api, name_key).await?;
    share_has_to_exist(share_id_seq, name_key, &msg)?;

    let (share_meta_seq, share_meta) = get_share_meta_by_id_or_err(kv_api, share_id, msg).await?;

    Ok((share_id_seq, share_id, share_meta_seq, share_meta))
}

/// Returns (share_meta_seq, share_meta)
pub async fn get_share_meta_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareMeta), KVAppError> {
    let id_key = ShareId { share_id };

    let (share_meta_seq, share_meta) = get_pb_value(kv_api, &id_key).await?;
    share_meta_has_to_exist(share_meta_seq, share_id, msg)?;

    Ok((share_meta_seq, share_meta.unwrap()))
}

fn share_meta_has_to_exist(seq: u64, share_id: u64, msg: impl Display) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, share_id = share_id; "share meta does not exist");

        Err(KVAppError::AppError(AppError::UnknownShareId(
            UnknownShareId::new(share_id, format!("{}: {}", msg, share_id)),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a share_id or share_meta exists by checking the seq.
///
/// Otherwise returns UnknownShare error
fn share_has_to_exist(
    seq: u64,
    share_name_ident: &ShareNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, share_name_ident :? =(share_name_ident); "share does not exist");

        Err(KVAppError::AppError(AppError::UnknownShare(
            UnknownShare::new(
                &share_name_ident.share_name,
                format!("{}: {}", msg, share_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Returns (share_account_meta_seq, share_account_meta)
pub async fn get_share_account_meta_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ShareConsumer,
    msg: impl Display,
) -> Result<(u64, ShareAccountMeta), KVAppError> {
    let (share_account_meta_seq, share_account_meta): (u64, Option<ShareAccountMeta>) =
        get_pb_value(kv_api, name_key).await?;
    share_account_meta_has_to_exist(share_account_meta_seq, name_key, msg)?;

    Ok((
        share_account_meta_seq,
        // Safe unwrap(): share_meta_seq > 0 implies share_meta is not None.
        share_account_meta.unwrap(),
    ))
}

/// Return OK if a share_id or share_account_meta exists by checking the seq.
///
/// Otherwise returns UnknownShareAccounts error
fn share_account_meta_has_to_exist(
    seq: u64,
    name_key: &ShareConsumer,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, name_key :? =(name_key); "share account does not exist");

        Err(KVAppError::AppError(AppError::UnknownShareAccounts(
            UnknownShareAccounts::new(
                &[name_key.tenant.clone()],
                name_key.share_id,
                format!("{}: {}", msg, name_key),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Returns (share_meta_seq, share_meta)
pub async fn get_share_id_to_name_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareNameIdent), KVAppError> {
    let id_key = ShareIdToName { share_id };

    let (share_name_seq, share_name) = get_pb_value(kv_api, &id_key).await?;
    if share_name_seq == 0 {
        debug!(share_name_seq = share_name_seq, share_id = share_id; "share meta does not exist");

        return Err(KVAppError::AppError(AppError::UnknownShareId(
            UnknownShareId::new(share_id, format!("{}: {}", msg, share_id)),
        )));
    }

    Ok((share_name_seq, share_name.unwrap()))
}

pub fn get_share_database_id_and_privilege(
    name_key: &ShareNameIdent,
    share_meta: &ShareMeta,
) -> Result<(u64, BitFlags<ShareGrantObjectPrivilege>), KVAppError> {
    if let Some(entry) = &share_meta.database {
        if let ShareGrantObject::Database(db_id) = entry.object {
            return Ok((db_id, entry.privileges));
        } else {
            unreachable!("database MUST be Database object");
        }
    }

    Err(KVAppError::AppError(AppError::ShareHasNoGrantedDatabase(
        ShareHasNoGrantedDatabase::new(name_key.tenant.name(), &name_key.share_name),
    )))
}

// Return true if all the database data has been removed.
pub async fn is_all_db_data_removed(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
) -> Result<bool, KVAppError> {
    let dbid = DatabaseId { db_id };

    let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) = get_pb_value(kv_api, &dbid).await?;
    debug_assert_eq!((db_meta_seq == 0), db_meta.is_none());
    if db_meta_seq != 0 {
        return Ok(false);
    }

    let id_to_name = DatabaseIdToName { db_id };
    let (name_ident_seq, name_ident): (_, Option<DatabaseNameIdent>) =
        get_pb_value(kv_api, &id_to_name).await?;
    debug_assert_eq!((name_ident_seq == 0), name_ident.is_none());
    if name_ident_seq != 0 {
        return Ok(false);
    }

    Ok(true)
}

// Return (true, `DataBaseMeta.from_share`) if the database needs to be removed, otherwise return (false, None).
// f: the predict function whether or not the database needs to be removed
//    base on the database meta passed by the user.
// When the database needs to be removed, add `TxnCondition` into `condition`
//    and `TxnOp` into the `if_then`.
pub async fn is_db_need_to_be_remove<F>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
    mut f: F,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(bool, Option<ShareNameIdent>), KVAppError>
where
    F: FnMut(&DatabaseMeta) -> bool,
{
    let dbid = DatabaseId { db_id };

    let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) = get_pb_value(kv_api, &dbid).await?;
    if db_meta_seq == 0 {
        return Ok((false, None));
    }

    let id_to_name = DatabaseIdToName { db_id };
    let (name_ident_seq, _name_ident): (_, Option<DatabaseNameIdent>) =
        get_pb_value(kv_api, &id_to_name).await?;
    if name_ident_seq == 0 {
        return Ok((false, None));
    }

    if let Some(db_meta) = db_meta {
        if f(&db_meta) {
            condition.push(txn_cond_seq(&dbid, Eq, db_meta_seq));
            if_then.push(txn_op_del(&dbid));
            condition.push(txn_cond_seq(&id_to_name, Eq, name_ident_seq));
            if_then.push(txn_op_del(&id_to_name));

            return Ok((true, db_meta.from_share));
        }
    }
    Ok((false, None))
}

pub async fn get_object_shared_by_share_ids(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    object: &ShareGrantObject,
) -> Result<(u64, ObjectSharedByShareIds), KVAppError> {
    let (seq, share_ids): (u64, Option<ObjectSharedByShareIds>) =
        get_pb_value(kv_api, object).await?;

    match share_ids {
        Some(share_ids) => Ok((seq, share_ids)),
        None => Ok((0, ObjectSharedByShareIds::default())),
    }
}

pub async fn get_table_names_by_ids(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ids: &[u64],
) -> Result<Vec<String>, KVAppError> {
    let mut table_names = vec![];

    let keys: Vec<String> = ids
        .iter()
        .map(|id| TableIdToName { table_id: *id }.to_string_key())
        .collect();

    let mut id_iter = ids.iter();

    for c in keys.chunks(DEFAULT_MGET_SIZE) {
        let table_seq_name: Vec<(u64, Option<DBIdTableName>)> = mget_pb_values(kv_api, c).await?;
        for (_seq, table_name_opt) in table_seq_name {
            let id = id_iter.next().unwrap();
            match table_name_opt {
                Some(table_name) => table_names.push(table_name.table_name),
                None => {
                    return Err(KVAppError::AppError(AppError::UnknownTableId(
                        UnknownTableId::new(*id, "get_table_names_by_ids"),
                    )));
                }
            }
        }
    }

    Ok(table_names)
}

pub async fn get_tableinfos_by_ids(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ids: &[u64],
    tenant_dbname: &DatabaseNameIdent,
    dbid_tbl_names: Option<Vec<DBIdTableName>>,
    db_type: DatabaseType,
) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
    let mut tb_meta_keys = Vec::with_capacity(ids.len());
    for id in ids.iter() {
        let table_id = TableId { table_id: *id };

        tb_meta_keys.push(table_id.to_string_key());
    }

    // mget() corresponding table_metas

    let mut seq_tbl_metas = Vec::with_capacity(ids.len());
    let chunk_size = DEFAULT_MGET_SIZE;

    for table_ids in tb_meta_keys.chunks(chunk_size) {
        let got = kv_api.mget_kv(table_ids).await?;
        seq_tbl_metas.extend(got);
    }

    let mut tbl_infos = Vec::with_capacity(ids.len());

    let tbl_names = match dbid_tbl_names {
        Some(dbid_tbnames) => Vec::<String>::from_iter(
            dbid_tbnames
                .into_iter()
                .map(|dbid_tbname| dbid_tbname.table_name),
        ),

        None => get_table_names_by_ids(kv_api, ids).await?,
    };

    for (i, seq_meta_opt) in seq_tbl_metas.iter().enumerate() {
        if let Some(seq_meta) = seq_meta_opt {
            let tbl_meta: TableMeta = deserialize_struct(&seq_meta.data)?;

            let tb_info = TableInfo {
                ident: TableIdent {
                    table_id: ids[i],
                    seq: seq_meta.seq,
                },
                desc: format!("'{}'.'{}'", tenant_dbname.db_name, tbl_names[i]),
                meta: tbl_meta,
                name: tbl_names[i].clone(),
                tenant: tenant_dbname.tenant.name().to_string(),
                db_type: db_type.clone(),
            };
            tbl_infos.push(Arc::new(tb_info));
        } else {
            debug!(
                k = &tb_meta_keys[i];
                "db_meta not found, maybe just deleted after listing names and before listing meta"
            );
        }
    }

    Ok(tbl_infos)
}

pub async fn list_tables_from_unshare_db(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
    tenant_dbname: &DatabaseNameIdent,
) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
    // List tables by tenant, db_id, table_name.

    let dbid_tbname = DBIdTableName {
        db_id,
        // Use empty name to scan all tables
        table_name: "".to_string(),
    };

    let (dbid_tbnames, ids) = list_u64_value(kv_api, &dbid_tbname).await?;

    get_tableinfos_by_ids(
        kv_api,
        &ids,
        tenant_dbname,
        Some(dbid_tbnames),
        DatabaseType::NormalDB,
    )
    .await
}

pub async fn list_tables_from_share_db(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share: ShareNameIdent,
    tenant_dbname: &DatabaseNameIdent,
) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
    let res = get_share_or_err(
        kv_api,
        &share,
        format!("list_tables_from_share_db: {}", &share),
    )
    .await;

    let (share_id_seq, _share_id, _share_meta_seq, share_meta) = match res {
        Ok(x) => x,
        Err(e) => {
            return Err(e);
        }
    };
    if share_id_seq == 0 {
        return Err(KVAppError::AppError(AppError::WrongShare(WrongShare::new(
            share.to_string_key(),
        ))));
    }

    let mut ids = Vec::with_capacity(share_meta.entries.len());
    for (_, entry) in share_meta.entries.iter() {
        if let ShareGrantObject::Table(table_id) = entry.object {
            ids.push(table_id);
        }
    }
    get_tableinfos_by_ids(
        kv_api,
        &ids,
        tenant_dbname,
        None,
        DatabaseType::ShareDB(share.into()),
    )
    .await
}

pub async fn convert_share_meta_to_spec(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_name: &str,
    share_id: u64,
    share_meta: ShareMeta,
) -> Result<ShareSpec, KVAppError> {
    let (database, db_privileges) = if let Some(database) = share_meta.database {
        if let ShareGrantObject::Database(db_id) = database.object {
            let id_key = DatabaseIdToName { db_id };

            let (_db_meta_seq, db_name): (_, Option<DatabaseNameIdent>) =
                get_pb_value(kv_api, &id_key).await?;
            if let Some(db_name) = db_name {
                (
                    Some(ShareDatabaseSpec {
                        name: db_name.db_name,
                        id: db_id,
                    }),
                    Some(database.privileges),
                )
            } else {
                (None, None)
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    let mut tables = vec![];
    for (_, entry) in share_meta.entries.iter() {
        if let ShareGrantObject::Table(table_id) = entry.object {
            let table_id_to_name_key = TableIdToName { table_id };
            let (_table_id_to_name_seq, table_name): (_, Option<DBIdTableName>) =
                get_pb_value(kv_api, &table_id_to_name_key).await?;
            if let Some(table_name) = table_name {
                tables.push(ShareTableSpec::new(
                    &table_name.table_name,
                    table_name.db_id,
                    table_id,
                ));
            }
        }
    }

    Ok(ShareSpec {
        name: share_name.to_owned(),
        share_id,
        version: 1,
        database,
        tables,
        tenants: Vec::from_iter(share_meta.accounts.into_iter()),
        db_privileges,
        comment: share_meta.comment.clone(),
        share_on: Some(share_meta.share_on),
    })
}

// return share name and new share meta
pub async fn remove_db_from_share(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    db_id: u64,
    db_name: &DatabaseNameIdent,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(String, ShareMeta), KVAppError> {
    let (_seq, share_name) = get_share_id_to_name_or_err(
        kv_api,
        share_id,
        format!("remove_db_from_share: {}", share_id),
    )
    .await?;

    let (share_meta_seq, mut share_meta) = get_share_meta_by_id_or_err(
        kv_api,
        share_id,
        format!("remove_db_from_share: {}", share_id),
    )
    .await?;

    match share_meta.database {
        Some(entry) => {
            if let ShareGrantObject::Database(share_db_id) = entry.object {
                if share_db_id != db_id {
                    return Err(KVAppError::AppError(AppError::WrongShareObject(
                        WrongShareObject::new(db_name.db_name.to_string()),
                    )));
                }
            }
        }
        None => {
            return Err(KVAppError::AppError(AppError::ShareHasNoGrantedDatabase(
                ShareHasNoGrantedDatabase::new(db_name.tenant.name(), &share_name.share_name),
            )));
        }
    }
    share_meta.database = None;
    share_meta.entries = BTreeMap::new();

    let id_key = ShareId { share_id };
    condition.push(txn_cond_seq(&id_key, Eq, share_meta_seq));
    if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?));

    Ok((share_name.share_name, share_meta))
}

// return (share name, new share meta, new share table info)
pub async fn remove_table_from_share(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    table_id: u64,
    tenant: &Tenant,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(String, ShareMeta, Option<TableInfoMap>), KVAppError> {
    let (_seq, share_name) = get_share_id_to_name_or_err(
        kv_api,
        share_id,
        format!("remove_table_from_share: {}", share_id),
    )
    .await?;

    let (share_meta_seq, mut share_meta) = get_share_meta_by_id_or_err(
        kv_api,
        share_id,
        format!("remove_table_from_share: {}", share_id),
    )
    .await?;

    let mut remove_table_name = None;
    let entries = share_meta.entries.clone();
    for (table_name, entry) in entries {
        if let ShareGrantObject::Table(share_table_id) = entry.object {
            if share_table_id == table_id {
                remove_table_name = Some(table_name);
                break;
            }
        }
    }
    match remove_table_name {
        Some(table_name) => {
            share_meta.entries.remove(&table_name);
        }
        None => {
            warn!(
                "remove_table_from_share: table-id {} not found of share {} in tenant {}",
                table_id,
                &share_name.share_name,
                tenant.name()
            );
        }
    }

    let id_key = ShareId { share_id };
    condition.push(txn_cond_seq(&id_key, Eq, share_meta_seq));
    if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?));

    let mut db_name = None;
    let mut shared_db_id = 0;
    if let Some(ref entry) = share_meta.database {
        if let ShareGrantObject::Database(db_id) = entry.object {
            let db_id_key = DatabaseIdToName { db_id };
            let (_db_name_seq, db_name_ident): (_, Option<DatabaseNameIdent>) =
                get_pb_value(kv_api, &db_id_key).await?;
            db_name = db_name_ident;
            shared_db_id = db_id;
        } else {
            return Err(KVAppError::AppError(AppError::ShareHasNoGrantedDatabase(
                ShareHasNoGrantedDatabase::new(tenant.name(), &share_name.share_name),
            )));
        }
    }

    let share_table_info = match db_name {
        Some(db_name) => {
            let mut table_ids = HashSet::new();
            for entry in share_meta.entries.values() {
                if let ShareGrantObject::Table(table_id) = entry.object {
                    table_ids.insert(table_id);
                } else {
                    unreachable!();
                }
            }
            let all_tables = list_tables_from_unshare_db(kv_api, shared_db_id, &db_name).await?;
            let table_infos = BTreeMap::from_iter(
                all_tables
                    .iter()
                    .filter(|table_info| table_ids.contains(&table_info.ident.table_id))
                    .map(|table_info| {
                        let mut table_info = table_info.as_ref().clone();
                        table_info.db_type = DatabaseType::ShareDB(share_name.clone().into());
                        (table_info.name.clone(), table_info)
                    })
                    .collect::<Vec<_>>(),
            );

            if table_infos.is_empty() {
                None
            } else {
                Some(table_infos)
            }
        }
        None => {
            return Err(KVAppError::AppError(AppError::ShareHasNoGrantedDatabase(
                ShareHasNoGrantedDatabase::new(tenant.name(), &share_name.share_name),
            )));
        }
    };

    Ok((share_name.share_name, share_meta, share_table_info))
}

pub async fn get_share_table_info(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_name: &ShareNameIdent,
    share_meta: &ShareMeta,
) -> Result<ShareTableInfoMap, KVAppError> {
    let mut db_name = None;
    let mut shared_db_id = 0;
    if let Some(ref entry) = share_meta.database {
        if let ShareGrantObject::Database(db_id) = entry.object {
            let db_id_key = DatabaseIdToName { db_id };
            let (_db_name_seq, db_name_ident): (_, Option<DatabaseNameIdent>) =
                get_pb_value(kv_api, &db_id_key).await?;
            db_name = db_name_ident;
            shared_db_id = db_id;
        } else {
            unreachable!();
        }
    }

    match db_name {
        Some(db_name) => {
            let mut table_ids = HashSet::new();
            for entry in share_meta.entries.values() {
                if let ShareGrantObject::Table(table_id) = entry.object {
                    table_ids.insert(table_id);
                } else {
                    unreachable!();
                }
            }
            let all_tables = list_tables_from_unshare_db(kv_api, shared_db_id, &db_name).await?;
            let table_infos = BTreeMap::from_iter(
                all_tables
                    .iter()
                    .filter(|table_info| table_ids.contains(&table_info.ident.table_id))
                    .map(|table_info| {
                        let mut table_info = table_info.as_ref().clone();
                        table_info.db_type = DatabaseType::ShareDB(share_name.clone().into());
                        (table_info.name.clone(), table_info)
                    })
                    .collect::<Vec<_>>(),
            );

            Ok((share_name.share_name.clone(), Some(table_infos)))
        }
        None => Ok((share_name.share_name.clone(), None)),
    }
}

pub async fn get_index_metas_by_ids(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    id_name_list: Vec<(u64, String)>,
) -> Result<Vec<(u64, String, IndexMeta)>, KVAppError> {
    let mut index_meta_keys = Vec::with_capacity(id_name_list.len());
    for (id, _) in id_name_list.iter() {
        let index_id = IndexId { index_id: *id };

        index_meta_keys.push(index_id.to_string_key());
    }

    let seq_index_metas = kv_api.mget_kv(&index_meta_keys).await?;

    let mut index_metas = Vec::with_capacity(id_name_list.len());

    for (i, ((id, name), seq_meta_opt)) in id_name_list
        .into_iter()
        .zip(seq_index_metas.iter())
        .enumerate()
    {
        if let Some(seq_meta) = seq_meta_opt {
            let index_meta: IndexMeta = deserialize_struct(&seq_meta.data)?;
            index_metas.push((id, name, index_meta));
        } else {
            debug!(k = &index_meta_keys[i]; "index_meta not found");
        }
    }

    Ok(index_metas)
}

/// Get `virtual_column_meta_seq` and [`VirtualColumnMeta`] by [`VirtualColumnNameIdent`],
/// or return [`AppError::VirtualColumnNotFound`] error wrapped in a [`KVAppError`] if not found.
pub async fn get_virtual_column_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &VirtualColumnNameIdent,
    ctx: impl Display + Copy,
) -> Result<(u64, VirtualColumnMeta), KVAppError> {
    let (seq, virtual_column_meta): (_, Option<VirtualColumnMeta>) =
        get_pb_value(kv_api, name_ident).await?;
    if virtual_column_meta.is_none() {
        return Err(KVAppError::AppError(AppError::VirtualColumnNotFound(
            VirtualColumnNotFound::new(
                name_ident.table_id,
                format!(
                    "get virtual column with tenant: {} table_id: {}",
                    name_ident.tenant, name_ident.table_id
                ),
            ),
        )));
    }

    let virtual_column_meta = virtual_column_meta.unwrap();

    debug!(
        ident :% =(name_ident),
        table_meta :? =(&virtual_column_meta);
        "{}",
        ctx
    );

    Ok((seq, virtual_column_meta))
}
