#!/bin/bash

set -o errexit
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
echo " === SCRIPT_PATH: $SCRIPT_PATH"
# go to work tree root
cd "$SCRIPT_PATH/../../"
ROOT="$(pwd)"
pwd

export RUST_BACKTRACE=full

BUILD_PROFILE="${BUILD_PROFILE:-debug}"

query_config_path="scripts/ci/deploy/config/databend-query-node-1.toml"

usage() {
    echo " === Assert that latest query being compatible with an old version query on fuse-table format"
    echo " === Expect ./bins/current contains current version binaries"
    echo " === Usage: $0 <old_version> <logictest_path> <supplementray_statless_test_path>"
}

source "${SCRIPT_PATH}/util.sh"


# -- main --

# The previous version to assert compatibility with
# e.g. old_query_ver="0.7.151"
old_query_ver="$1"

# default sqllogic test suite is "tests/fuse-compat/compat-logictest/"
logictest_path=${2:-"./base"}

# supplementary stateless test suite if provided (optional), which will be searched under "tests/fuse-compat/compat-stateless"
stateless_test_path="$3"

echo " === old query ver : ${old_query_ver}"
echo " === sql logic test path: ${logictest_path}"
echo " === supplementary stateless test path: ${stateless_test_path}"


chmod +x ./bins/current/*

echo " === current metasrv ver: $(./bins/current/databend-meta --single --cmd ver | tr '\n' ' ')"
echo " === current   query ver: $(./bins/current/databend-query --cmd ver | tr '\n' ' ')"
echo " === old query ver: $old_query_ver"


mkdir -p ./target/${BUILD_PROFILE}/

download_query_config "$old_query_ver" old_config
download_binary "$old_query_ver"

old_config_path="old_config/$query_config_path"
run_test $old_query_ver $old_config_path $logictest_path

if [ -n "$stateless_test_path" ];
then
  echo "=== running supplementary stateless test: ${stateless_test_path}"
  run_stateless $stateless_test_path
fi
