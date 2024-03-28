#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT
echo "create database db_stream" | $BENDSQL_CLIENT_CONNECT
echo "create table db_stream.base(a int)" | $BENDSQL_CLIENT_CONNECT
echo "insert into db_stream.base values(1)" | $BENDSQL_CLIENT_CONNECT
echo "alter table db_stream.base set options(change_tracking = true)" | $BENDSQL_CLIENT_CONNECT
echo "insert into db_stream.base values(2)" | $BENDSQL_CLIENT_CONNECT
echo "insert into db_stream.base values(3)" | $BENDSQL_CLIENT_CONNECT

BASE_ROW_ID=$(echo "select _base_row_id from db_stream.base where a = 3" | $BENDSQL_CLIENT_CONNECT)

SNAPSHOT_ID_1=$(echo "select snapshot_id from fuse_snapshot('db_stream','base') where row_count=1 limit 1" | $BENDSQL_CLIENT_CONNECT)
echo "create stream db_stream.s1 on table db_stream.base at (snapshot => '$SNAPSHOT_ID_1')" | $BENDSQL_CLIENT_CONNECT

SNAPSHOT_ID_2=$(echo "select snapshot_id from fuse_snapshot('db_stream','base') where row_count=2 limit 1" | $BENDSQL_CLIENT_CONNECT)
echo "create stream db_stream.s2 on table db_stream.base at (snapshot => '$SNAPSHOT_ID_2')" | $BENDSQL_CLIENT_CONNECT
echo "select a, change\$action, change\$is_update, change\$row_id='$BASE_ROW_ID' from db_stream.s2" | $BENDSQL_CLIENT_CONNECT
echo "select a from db_stream.base at (stream => db_stream.s2) order by a" | $BENDSQL_CLIENT_CONNECT

TIMEPOINT_1=$(echo "select timestamp from fuse_snapshot('db_stream','base') where row_count=1 limit 1" | $BENDSQL_CLIENT_CONNECT)
echo "create stream db_stream.t1 on table db_stream.base at (timestamp => '$TIMEPOINT_1'::TIMESTAMP)" | $BENDSQL_CLIENT_CONNECT

echo "alter table db_stream.base set options(change_tracking = true)" | $BENDSQL_CLIENT_CONNECT
TIMEPOINT_2=$(echo "select timestamp from fuse_snapshot('db_stream','base') where row_count=2 limit 1" | $BENDSQL_CLIENT_CONNECT)
echo "create stream db_stream.t2 on table db_stream.base at (timestamp => '$TIMEPOINT_2'::TIMESTAMP)" | $BENDSQL_CLIENT_CONNECT
echo "select a, change\$action, change\$is_update, change\$row_id='$BASE_ROW_ID' from db_stream.t2" | $BENDSQL_CLIENT_CONNECT

echo "alter table db_stream.base set options(change_tracking = false)" | $BENDSQL_CLIENT_CONNECT
echo "create stream db_stream.s3 on table db_stream.base at (snapshot => '$SNAPSHOT_ID_2')" | $BENDSQL_CLIENT_CONNECT
echo "alter table db_stream.base set options(change_tracking = true)" | $BENDSQL_CLIENT_CONNECT
echo "create stream db_stream.s4 on table db_stream.base at (stream => db_stream.s2)" | $BENDSQL_CLIENT_CONNECT
echo "select a from db_stream.s2" | $BENDSQL_CLIENT_CONNECT

echo "select count(*) from fuse_snapshot('db_stream', 'base')" | $BENDSQL_CLIENT_CONNECT
echo "set data_retention_time_in_days=0; optimize table db_stream.base purge before (stream => db_stream.s2)" | $BENDSQL_CLIENT_CONNECT
echo "select count(*) from fuse_snapshot('db_stream', 'base')" | $BENDSQL_CLIENT_CONNECT

echo "drop stream if exists db_stream.s2" | $BENDSQL_CLIENT_CONNECT
echo "drop stream if exists db_stream.t2" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists db_stream.base all" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT
