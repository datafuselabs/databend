#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

rm -rf /tmp/00_0016
mkdir -p /tmp/00_0016

stmt "drop table if exists t16"
stmt "create table t16(a int, b int)"

stmt "drop stage if exists s16"
stmt "create stage s16 url='fs:///tmp/00_0016/';"
stmt "unset global purge_duplicated_files_in_copy;"

# setup
# copy one file from test stage @s16, with purge enabled
cat << EOF > /tmp/00_0016/i1.csv
1,1
2,2
EOF

query "copy into t16 from @s16 file_format = (type = CSV) purge = true"

query "list @s16"

stmt "select * from t16"

# case 1:
# put the previously copied file i1
# and a new file i2 into the test stage @s16,
# then copy into table t16 from the test stage location,
# and expects that only the new file i2 will be copied

echo "duplicate file i1 into the stage"
cat << EOF > /tmp/00_0016/i1.csv
1,1
2,2
EOF

echo "put the new file i2 into the stage"
cat << EOF > /tmp/00_0016/i2.csv
3,3
4,4
EOF

echo "enable purge_duplicated_files_in_copy and copy into from location again"
query "set purge_duplicated_files_in_copy =1; select name,value,default,level from system.settings where name='purge_duplicated_files_in_copy'; copy into t16 from @s16 file_format = (type = CSV) purge = true"

echo "stage should be empty, the duplicated file i1 should be removed"
query "list @s16"

echo "tow new rows from new file i2 should be copied into table t16"
stmt "select * from t16 order by a"

# case 2:
# even if there are only duplicated files in the stage,
# these duplicated files should be removed from the stage

echo "put the same files into the stage"
cat << EOF > /tmp/00_0016/i1.csv
1,1
2,2
EOF

cat << EOF > /tmp/00_0016/i2.csv
3,3
4,4
EOF

echo "enable purge_duplicated_files_in_copy and copy into from location again"
query "set purge_duplicated_files_in_copy =1;select name,value,default,level from system.settings where name='purge_duplicated_files_in_copy'; copy into t16 from @s16 file_format = (type = CSV) purge = true"

echo "stage should be empty"
query "list @s16"

echo "table should be unchanged"
stmt "select * from t16 order by a"

# case 3:
# if the "purge" option is not enabled, the duplicated files should not be removed from the stage,
# even if the "purge_duplicated_files_in_copy" option is enabled.
# note: this will trigger the "no files to be copied" shortcut

echo "put the same files into the stage"
cat << EOF > /tmp/00_0016/i1.csv
1,1
2,2
EOF

cat << EOF > /tmp/00_0016/i2.csv
3,3
4,4
EOF

echo "enable purge_duplicated_files_in_copy, but disable the purge option, then copy into from location again"
query "set purge_duplicated_files_in_copy =1; select name,value,default,level from system.settings where name='purge_duplicated_files_in_copy'; copy into t16 from @s16 file_format = (type = CSV) purge = false"

echo "stage should not be empty, contains i1 and i2"
query "list @s16" | awk '{print $1}' | sort

echo "table should be unchanged"
stmt "select * from t16 order by a"

# case 3:
# if the "purge" option is not enabled, the duplicated files should not be removed from the stage,
# even if the "purge_duplicated_files_in_copy" option is enabled

echo "put the same files into the stage"
cat << EOF > /tmp/00_0016/i1.csv
1,1
2,2
EOF

cat << EOF > /tmp/00_0016/i2.csv
3,3
4,4
EOF

echo "also put the a new file into the stage"
cat << EOF > /tmp/00_0016/i3.csv
5,5
6,6
EOF

echo "enable purge_duplicated_files_in_copy, but disable the purge option, then copy into from location again"
query "set purge_duplicated_files_in_copy =1; select name,value,default,level from system.settings where name='purge_duplicated_files_in_copy'; copy into t16 from @s16 file_format = (type = CSV) purge = false"

echo "stage should not be empty, contains i1, i2, and i3"
query "list @s16" | awk '{print $1}' | sort

echo "tow new rows should be copied into table t16"
stmt "select * from t16 order by a"

# cleanup
stmt "drop table if exists t16"
stmt "drop stage if exists s16"

