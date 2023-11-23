#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# this case is supposed to run with the current version of query,
# after fuse-compat/compat-logictest/01_meta_compression_v3_to_v4/ been run with the old version of query

##########################################
# test flashback across version boundary #
##########################################

echo "suite: flashback across version boundary"
# current situation:
# - we have inserted 3 rows into fuse_test_flashback, which produced 3 snapshots
# - and the content of the table is {1,2,3}


echo "checking that 3 snapshots exist"
echo "select count(*)  from fuse_snapshot('default', 'fuse_test_flashback')" | $BENDSQL_CLIENT_CONNECT

# write down 2 new rows using current version
echo "insert into fuse_test_flashback values (4)" | $BENDSQL_CLIENT_CONNECT
echo "insert into fuse_test_flashback values (5)" | $BENDSQL_CLIENT_CONNECT

# the table now contains five rows {1,2,3,4,5}, and 5 snapshots:
# s1 {1}, s2 {1,2}, s3 {1,2,3}, s4 {1,2,3,4}, s5 {1,2,3,4,5}
# s1, s2, s3 are of table snapshot format version 3
# s4, s5 are of table snapshot format version 4
echo "checking that 5 snapshot exist"
echo "select count(*)  from fuse_snapshot('default', 'fuse_test_flashback')" | $BENDSQL_CLIENT_CONNECT

# flashback to the 2nd last snapshot
FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=4" | $BENDSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the 2nd last snapshot s4 which is of version 4, and now the table contains {1,2,3,4}"
echo "select c  from fuse_test_flashback order by c" | $BENDSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=3" | $BENDSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the 3rd last snapshot s3 which is of version 4, and the table contains {1,2,3}"
echo "select c  from fuse_test_flashback order by c" | $BENDSQL_CLIENT_CONNECT

# crossing the version boundary

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=2" | $BENDSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the 4th last snapshot s2 which is of version 3, the table contains {1,2}"
echo "select c  from fuse_test_flashback order by c" | $BENDSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=1" | $BENDSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the 5th last snapshot s1 which is of version 3, the table contains {1}"
echo "select c  from fuse_test_flashback order by c" | $BENDSQL_CLIENT_CONNECT

###########################################################
# test compaction & fuse_snapshot across version boundary #
# issue 11204                                             #
###########################################################

echo "suite: test compaction & fuse_snapshot across version boundary"

# current situation:
# - we have inserted 2 rows into fuse_test_compaction, which produced 2 snapshots
# - the content of the table is {1,2}
# - the snapshots are s1 {1}, s2 {1,2}


echo "checking that 2 snapshots of version 3 exist"
echo "select count() c, format_version v from fuse_snapshot('default', 'fuse_test_compaction') group by v order by c" | $BENDSQL_CLIENT_CONNECT

# grab the current last snapshot (s2)
FST_SNAPSHOT_S2_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_compaction') limit 1" | $BENDSQL_CLIENT_CONNECT)
# grab s2's location
FST_SNAPSHOT_S2_ID_LOC=$(echo "select snapshot_location from fuse_snapshot('default','fuse_test_compaction') limit 1" | $BENDSQL_CLIENT_CONNECT)

# compact the table
echo "doing compact"
echo "optimize table fuse_test_compaction compact" | $BENDSQL_CLIENT_CONNECT

# verify the following cases:

echo "checking that after compaction the table still contains {1,2}"
echo "select c from fuse_test_compaction order by c" | $BENDSQL_CLIENT_CONNECT

echo "checking that after compaction, 2 snapshots of version 3, and 1 snapshot of version 4 exist"
echo "select count() c, format_version v from fuse_snapshot('default', 'fuse_test_compaction') group by v order by c " | $BENDSQL_CLIENT_CONNECT

echo "checking the version and location of snapshots s2 is correct"
echo "select snapshot_location='${FST_SNAPSHOT_S2_ID_LOC}', format_version=3 from fuse_snapshot('default', 'fuse_test_compaction') where snapshot_id='$FST_SNAPSHOT_S2_ID'" | $BENDSQL_CLIENT_CONNECT

# flash back to s2
echo "alter table fuse_test_compaction flashback to (snapshot => '$FST_SNAPSHOT_S2_ID')" | $BENDSQL_CLIENT_CONNECT
echo "checking that flashback works as expected (to s2)"
echo "select snapshot_location='${FST_SNAPSHOT_S2_ID_LOC}', format_version=3 from fuse_snapshot('default', 'fuse_test_compaction') limit 1" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to s2,  the table contains {1,2}"
echo "select c  from fuse_test_compaction order by c" | $BENDSQL_CLIENT_CONNECT


###########################################
# mixed versioned segment compaction test #
###########################################

echo "suite: mixed versioned segment compaction test"

# compaction of mixed versioned segments in to new versioned segment

# current situation:
# - table option block_per_segment is 2
# - we have inserted 3 rows into t2, which produced 3 snapshots of version 3
# - the snapshots are s1 {1}, s2 {1,2}, s3 {1,2,3}
# - after that, we perform a compaction on s3, which give us a snapshot s4 {1,2,3}, of version 4
#   since block_per_segment is 2, s4 contains 2 segments, v3 segment_1: {1,2}, v4 segment_2: {3}
# end of description of s4

# creation of s5:
#---------------
# insert another row, which will produce a new snapshot s5 {1,2,3,4}, of version 3
echo "insert into t2 values (4)" | $BENDSQL_CLIENT_CONNECT

# s5 now contains 3 segments, 2 of version 3, and 1 of version 4
# - v2 segment_1: {1,2}, v2 segment_2: {3}, v3 segment_3: {4}

# creation of s6: the mixed version segments compaction,
#---------------
# compact the segments again.
#
# note that we should use `compact segment` here, otherwise if `compact` is used,
# the blocks will also be compacted, which produces two new segments of version 3.
echo "optimize table t2 compact segment" | $BENDSQL_CLIENT_CONNECT

# according to the table options segment_per_block=3,
# v3 segment_2 and v4 segment_3 should be compacted -- the mixed version segments compaction,
# into a new segment of version 4.
#
# the new snapshot s6 produced by this compaction should contains 2 segments:
# - v3 segment_1: {1,2}, v4 segment_2: {3, 4}

# grab the id of s6
FST_SNAPSHOT_S6_ID=$(echo "select snapshot_id from fuse_snapshot('default','t2') limit 1" | $BENDSQL_CLIENT_CONNECT)

echo "check segments after compaction, there should be 2 segments, a version v3 and a version v5"
echo "select count() c, format_version v from fuse_segment('default', 't2', '$FST_SNAPSHOT_S6_ID') group by v order by v " | $BENDSQL_CLIENT_CONNECT

echo "check table contains {1,2,3,4} after compaction"
echo "select * from t2 order by c" | $BENDSQL_CLIENT_CONNECT

