#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# contrib/lance_fdw/ci/prepare-tests.sh - give the regression suites their data.
#
# The suites read Lance datasets from a local directory and from an S3 bucket.
# This generates the datasets with pylance, starts a SeaweedFS S3 gateway on
# this host, uploads the datasets to it, and writes test/regress.mk, which the
# Makefile reads, with where both are.
#
# Usage: prepare-tests.sh [OWNER]   (as root; OWNER, default gpadmin, is the
#                                    account the cluster runs as)

set -euo pipefail

SEAWEEDFS_VERSION=${SEAWEEDFS_VERSION:-4.47}
S3_PORT=${S3_PORT:-8333}
S3_BUCKET=lance-regress
S3_REGION=us-east-1
S3_KEY=lance-ci
S3_SECRET=lance-ci-secret

owner=${1:-gpadmin}
here=$(cd "$(dirname "$0")/.." && pwd)
fixtures=$here/test/fixtures
state=${LANCE_CI_STATE:-/tmp/lance-ci}
mkdir -p "$state"

# pylance and numpy want a newer Python than the build image's own.
if ! command -v python3.12 >/dev/null; then
	dnf -y -q install python3.12 python3.12-pip
fi
python3.12 -m venv "$state/venv"
"$state/venv/bin/pip" install -q -r "$fixtures/requirements.txt"

echo "prepare-tests: generating the datasets"
"$state/venv/bin/python" "$fixtures/gen_fixtures.py"

case $(uname -m) in
	x86_64) weed_arch=linux_amd64 ;;
	aarch64) weed_arch=linux_arm64 ;;
	*) echo "prepare-tests: no SeaweedFS build for $(uname -m)" >&2; exit 1 ;;
esac
curl -fsSL --retry 5 --retry-connrefused \
	"https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/${weed_arch}.tar.gz" |
	tar -xz -C "$state" weed

# Without an identity file SeaweedFS accepts any credentials, and the suites
# that check a wrong secret is refused would pass for the wrong reason.
cat > "$state/s3.json" <<JSON
{"identities":[{"name":"lance","credentials":[{"accessKey":"$S3_KEY","secretKey":"$S3_SECRET"}],
  "actions":["Admin","Read","Write","List","Tagging"]}]}
JSON
mkdir -p "$state/s3-data"
# setsid, so that the gateway outlives the CI step that started it.
setsid "$state/weed" server -dir="$state/s3-data" -ip=127.0.0.1 \
	-s3 -s3.port="$S3_PORT" -s3.config="$state/s3.json" \
	>"$state/s3.log" 2>&1 < /dev/null &

for _ in $(seq 60); do
	status=$(curl --noproxy '*' -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$S3_PORT/" || true)
	[[ -n "$status" && "$status" != 000 ]] && break
	sleep 1
done
if [[ -z "$status" || "$status" == 000 ]]; then
	echo "prepare-tests: the S3 gateway never started" >&2
	tail -50 "$state/s3.log" >&2
	exit 1
fi
for _ in $(seq 30); do
	echo "s3.bucket.create -name $S3_BUCKET" | "$state/weed" shell -master=127.0.0.1:9333 >/dev/null 2>&1 || true
	echo 's3.bucket.list' | "$state/weed" shell -master=127.0.0.1:9333 2>/dev/null | grep -q "$S3_BUCKET" && break
	sleep 1
done

echo "prepare-tests: uploading the datasets to s3://$S3_BUCKET/fixtures/"
LANCE_S3_ENDPOINT_HOST=http://127.0.0.1:$S3_PORT LANCE_S3_BUCKET=$S3_BUCKET \
LANCE_S3_REGION=$S3_REGION LANCE_S3_KEY=$S3_KEY LANCE_S3_SECRET=$S3_SECRET \
	"$state/venv/bin/python" "$fixtures/upload_minio.py" --no-env-file

cat > "$here/test/regress.mk" <<MK
LANCE_FIXTURE_DIR = $fixtures/data
LANCE_S3_ENDPOINT = http://127.0.0.1:$S3_PORT
LANCE_S3_BUCKET = $S3_BUCKET
LANCE_S3_REGION = $S3_REGION
LANCE_S3_KEY = $S3_KEY
LANCE_S3_SECRET = $S3_SECRET
MK
chown -R "$owner": "$fixtures/data" "$here/test/regress.mk"
echo "prepare-tests: done"
