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
# contrib/lance_fdw/ci/build-lance-c.sh - build liblance_c for CI.
#
# lance_fdw links liblance_c, which is written in Rust and is not part of the
# Apache Cloudberry source tree.  This builds the tested revision from source
# and installs its header and library under PREFIX:
#
#   PREFIX/include/lance/lance.h
#   PREFIX/lib/liblance_c.so
#
# Usage: build-lance-c.sh PREFIX      (as root: it installs protoc and Rust)
# A PREFIX that already holds the library, e.g. restored from a cache, is kept.

set -euo pipefail

# The lance-c revision lance_fdw is tested against.  Its C ABI is not stable
# yet, so this moves only together with the code that uses it.
LANCE_C_REPO=${LANCE_C_REPO:-https://github.com/lance-format/lance-c}
LANCE_C_COMMIT=${LANCE_C_COMMIT:-c43774ce81a2351e61c72a9065a76bd72a727673}
RUST_VERSION=${RUST_VERSION:-1.91.0}
PROTOC_VERSION=${PROTOC_VERSION:-24.3}

prefix=${1:?usage: build-lance-c.sh PREFIX}

if [[ -f "$prefix/lib/liblance_c.so" && -f "$prefix/include/lance/lance.h" ]]; then
	echo "build-lance-c: $prefix already holds liblance_c, nothing to build"
	exit 0
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

case $(uname -m) in
	x86_64) protoc_arch=x86_64 ;;
	aarch64) protoc_arch=aarch_64 ;;
	*) echo "build-lance-c: no protoc build for $(uname -m)" >&2; exit 1 ;;
esac
curl -fsSL --retry 5 --retry-connrefused -o "$work/protoc.zip" \
	"https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip"
unzip -q -o "$work/protoc.zip" -d "$work/protoc" bin/protoc 'include/*'
export PROTOC=$work/protoc/bin/protoc
"$PROTOC" --version

export RUSTUP_HOME=$work/rustup CARGO_HOME=$work/cargo
curl -fsSL --retry 5 --retry-connrefused https://sh.rustup.rs |
	sh -s -- -y --no-modify-path --profile minimal --default-toolchain "$RUST_VERSION"
export PATH=$CARGO_HOME/bin:$PATH
rustc --version

git init -q "$work/lance-c"
git -C "$work/lance-c" fetch -q --depth 1 "$LANCE_C_REPO" "$LANCE_C_COMMIT"
git -C "$work/lance-c" checkout -q FETCH_HEAD
echo "build-lance-c: building lance-c $LANCE_C_COMMIT"
(cd "$work/lance-c" && cargo build --release --locked)

install -d "$prefix/include/lance" "$prefix/lib"
install -m 0644 "$work/lance-c/include/lance/"*.h "$prefix/include/lance/"
install -m 0755 "$work/lance-c/target/release/liblance_c.so" "$prefix/lib/"
strip --strip-unneeded "$prefix/lib/liblance_c.so"
ls -l "$prefix/lib/liblance_c.so" "$prefix/include/lance"
