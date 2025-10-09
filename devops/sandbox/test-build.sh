#!/usr/bin/env bash
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------
# Test script for verifying the refactored sandbox Dockerfiles
# --------------------------------------------------------------------

set -euo pipefail

echo "Testing refactored sandbox Dockerfiles..."

# Test 1: Build the main branch Dockerfile
echo "Test 1: Building main branch Dockerfile..."
docker build --file Dockerfile.main.rockylinux9 \
             --build-arg TIMEZONE_VAR="America/Los_Angeles" \
             --tag cbdb-test-main:rockylinux9 .

# Test 2: Build the release Dockerfile
echo "Test 2: Building release Dockerfile..."
docker build --file Dockerfile.RELEASE.rockylinux9 \
             --build-arg TIMEZONE_VAR="America/Los_Angeles" \
             --build-arg CODEBASE_VERSION_VAR="2.0.0" \
             --tag cbdb-test-release:rockylinux9 .

echo "All tests passed! Dockerfiles are working correctly."