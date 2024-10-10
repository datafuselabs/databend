#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "*************************************"
echo "* Setting STORAGE_TYPE to S3.       *"
echo "*                                   *"
echo "* Please make sure that S3 backend  *"
echo "* is ready, and configured properly.*"
echo "*************************************"
export STORAGE_TYPE=s3
export STORAGE_S3_BUCKET=testbucket
export STORAGE_S3_ROOT=admin
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
export STORAGE_S3_ACCESS_KEY_ID=minioadmin
export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
export STORAGE_ALLOW_INSECURE=true

echo "Install dependence"
python3 -m pip install --quiet mysql-connector-python

echo "Build dateoffset"
git clone https://github.com/batiati/dateoffset
pushd dateoffset
make
popd


echo "Starting standalone DatabendQuery(faked time: 2 days ago)"
LD_PRELOAD=./dateoffset/dateoffset.so \
DATE_OFFSET=$(date -d "-2 days" '+%s') \
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
pushd "$SCRIPT_PATH/../../tests" || exit

echo "Preparing data (faked time)"
./databend-test --mode 'standalone' --run-dir 8_faked_time_prepare

popd
echo "Starting standalone DatabendQuery (Normal time)"
./scripts/ci/deploy/databend-query-standalone.sh

pushd "$SCRIPT_PATH/../../tests" || exit

echo "Testing"
./databend-test --mode 'standalone' --run-dir 9_faked_time
