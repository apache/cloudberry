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
#
# Everything it downloads is pinned and checked: lance-c by commit, Cargo's
# dependencies by its lock file, and protoc and rustup-init by SHA-256.

set -euo pipefail

# The lance-c revision lance_fdw is tested against.  Its C ABI is not stable
# yet, so this moves only together with the code that uses it.
LANCE_C_REPO=${LANCE_C_REPO:-https://github.com/lance-format/lance-c}
LANCE_C_COMMIT=${LANCE_C_COMMIT:-c43774ce81a2351e61c72a9065a76bd72a727673}
RUST_VERSION=${RUST_VERSION:-1.91.0}
RUSTUP_VERSION=1.28.2
PROTOC_VERSION=24.3

prefix=${1:?usage: build-lance-c.sh PREFIX}

if [[ -f "$prefix/lib/liblance_c.so" && -f "$prefix/include/lance/lance.h" ]]; then
	echo "build-lance-c: $prefix already holds liblance_c, nothing to build"
	exit 0
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

case $(uname -m) in
	x86_64)
		protoc_arch=x86_64
		protoc_sha256=fc793561283d9ea6813fb757ae54f1afea6770afcd930904bdf3e590910420aa
		rust_target=x86_64-unknown-linux-gnu
		rustup_sha256=20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c
		;;
	aarch64)
		protoc_arch=aarch_64
		protoc_sha256=77a5a41f3e9712af6a35de13143b9b2b77f075aa1ab18a63cca4483b30f6e3cd
		rust_target=aarch64-unknown-linux-gnu
		rustup_sha256=e3853c5a252fca15252d07cb23a1bdd9377a8c6f3efa01531109281ae47f841c
		;;
	*) echo "build-lance-c: no protoc or Rust build for $(uname -m)" >&2; exit 1 ;;
esac

# download URL FILE SHA256
download() {
	curl -fsSL --retry 5 --retry-connrefused -o "$2" "$1"
	echo "$3  $2" | sha256sum -c --quiet - || {
		echo "build-lance-c: $1 does not have the expected SHA-256" >&2
		exit 1
	}
}

# Some of the .proto files lance-c's dependencies compile use proto3
# optional fields, and not all of their build scripts pass
# --experimental_allow_proto3_optional, so protoc has to be 3.15 or newer.
# Of the build images only Rocky 10's is (3.19); Rocky 9's 3.14 fails on
# substrait's type.proto, and Rocky 8 has 3.5.  Those download protoc.
protoc_ok() {
	local v
	v=$(protoc --version 2>/dev/null | awk '{print $2}') || return 1
	[[ -n "$v" ]] || return 1
	printf '%s\n3.15\n' "$v" | sort -V -C -r
}
if protoc_ok; then
	PROTOC=$(command -v protoc)
else
	download "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip" \
		"$work/protoc.zip" "$protoc_sha256"
	unzip -q -o "$work/protoc.zip" -d "$work/protoc" bin/protoc 'include/*'
	PROTOC=$work/protoc/bin/protoc
fi
export PROTOC
echo "build-lance-c: $PROTOC is $("$PROTOC" --version)"

export RUSTUP_HOME=$work/rustup CARGO_HOME=$work/cargo
download "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${rust_target}/rustup-init" \
	"$work/rustup-init" "$rustup_sha256"
chmod +x "$work/rustup-init"
"$work/rustup-init" -y --no-modify-path --profile minimal --default-toolchain "$RUST_VERSION"
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
