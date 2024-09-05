#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

SEGMENTS=$(echo "select file_location from fuse_segment('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
BLOCKS=$(echo "select block_location from fuse_block('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)

stmt "insert into test_vacuum2 values(2);"

IFS=$'\n' read -d '' -r -a segments <<< "$SEGMENTS"
IFS=$'\n' read -d '' -r -a blocks <<< "$BLOCKS"
blooms=()
for block in "${blocks[@]}"; do
    bloom=$(echo "$block" | sed -E 's|(1/[0-9]+)/_b/g([0-9a-f]{32})_v2\.parquet|\1/_i_b_v2/\2_v4.parquet|')
    blooms+=("$bloom")
done

stmt "set data_retention_time_in_days = 2;truncate table test_vacuum2;"

SNAPSHOTS=$(echo "select snapshot_location from fuse_snapshot('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
IFS=$'\n' read -d '' -r -a snapshots <<< "$SNAPSHOTS"
to_be_vacuumed=("${snapshots[@]}" "${segments[@]}" "${blocks[@]}" "${blooms[@]}")

# gc root
stmt "insert into test_vacuum2 values(3);"


# should have 4 snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

RESULTS=$(echo "set data_retention_time_in_days = 0;select * from fuse_vacuum2('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
IFS=$'\n' read -d '' -r -a results <<< "$RESULTS"

# verify the vacuum result
sorted_results=($(printf "%s\n" "${results[@]}" | sort))
sorted_to_be_vacuumed=($(printf "%s\n" "${to_be_vacuumed[@]}" | sort))

if [ "$(printf "%s" "${sorted_results[@]}")" != "$(printf "%s" "${sorted_to_be_vacuumed[@]}")" ]; then
    echo "Vacuum failed"
    echo "Results array: ${sorted_results[@]}"
    echo "To be vacuumed array: ${sorted_to_be_vacuumed[@]}"
    exit 1
fi

# remain two snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

# verify the data
query "select * from test_vacuum2;"

# restore default value
stmt "set data_retention_time_in_days = 1;"