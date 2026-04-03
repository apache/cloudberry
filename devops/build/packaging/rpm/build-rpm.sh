#!/bin/bash
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
# Script Name: build-rpm.sh
#
# Description:
# This script automates the process of building an RPM package using a specified
# version and release number. It ensures that the necessary tools are installed
# and that the spec file exists before attempting to build the RPM. The script
# also includes error handling to provide meaningful feedback in case of failure.
#
# Usage:
# ./build-rpm.sh -v <version> [-r <release>] [-d|--with-debug] [-h] [--dry-run]
#
# Options:
#   -v, --version <version>    : Specify the version (required)
#   -r, --release <release>    : Specify the release (optional, default is 1)
#   -d, --with-debug           : Build with debug symbols (optional)
#   -h, --help                 : Display this help and exit
#   -n, --dry-run              : Show what would be done, without making any changes
#
# Example:
#   ./build-rpm.sh -v 1.5.5 -r 2          # Build with version 1.5.5 and release 2
#   ./build-rpm.sh -v 1.5.5               # Build with version 1.5.5 and default release 1
#   ./build-rpm.sh -v 1.5.5 --with-debug  # Build with debug symbols
#
# Prerequisites:
# - The rpm-build package must be installed (provides the rpmbuild command).
# - The spec file must exist at ~/rpmbuild/SPECS/apache-cloudberry-db-incubating.spec.
#
# Error Handling:
# The script includes checks to ensure:
# - The version option (-v or --version) is provided.
# - The necessary commands are available.
# - The spec file exists at the specified location.
# If any of these checks fail, the script exits with an appropriate error message.

# Enable strict mode for better error handling
set -euo pipefail

# Default values
VERSION=""
RELEASE="1"
DEBUG_BUILD=false

# Function to display usage information
usage() {
  echo "Usage: $0 -v <version> [-r <release>] [-h] [--dry-run]"
  echo "  -v, --version <version>    : Specify the version (required)"
  echo "  -r, --release <release>    : Specify the release (optional, default is 1)"
  echo "  -d, --with-debug           : Build with debug symbols (optional)"
  echo "  -h, --help                 : Display this help and exit"
  echo "  -n, --dry-run              : Show what would be done, without making any changes"
  exit 1
}

# Function to check if required commands are available
check_commands() {
  local cmds=("rpmbuild")
  for cmd in "${cmds[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
      echo "Error: Required command '$cmd' not found. Please install it before running the script."
      exit 1
    fi
  done
}

# Parse options
while [[ "$#" -gt 0 ]]; do
  case $1 in
    -v|--version)
      VERSION="$2"
      shift 2
      ;;
    -r|--release)
      RELEASE="$2"
      shift 2
      ;;
    -d|--with-debug)
      DEBUG_BUILD=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    -n|--dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      echo "Unknown option: ($1)"
      shift
      ;;
  esac
done

# Ensure version is provided
if [ -z "$VERSION" ]; then
  echo "Error: Version (-v or --version) is required."
  usage
fi

# Check if required commands are available
check_commands

# Define the source spec file path (assuming it is in the same directory as the script)
SOURCE_SPEC_FILE="$(dirname "$0")/apache-cloudberry-db-incubating.spec"

# Ensure rpmbuild SPECS and SOURCES directories exist
mkdir -p ~/rpmbuild/SPECS
mkdir -p ~/rpmbuild/SOURCES

# Find project root (assumed to be four levels up from scripts directory: devops/build/packaging/rpm/)
PROJECT_ROOT="$(cd "$(dirname "$0")/../../../../" && pwd)"

# Define the target spec file path
SPEC_FILE=~/rpmbuild/SPECS/apache-cloudberry-db-incubating.spec

# Copy the spec file to rpmbuild/SPECS if the source exists and is different
if [ -f "$SOURCE_SPEC_FILE" ]; then
  # Avoid copying if SPEC_FILE is already a symlink/file pointing to SOURCE_SPEC_FILE (common in CI)
  if [ ! "$SOURCE_SPEC_FILE" -ef "$SPEC_FILE" ]; then
    cp -f "$SOURCE_SPEC_FILE" "$SPEC_FILE"
  fi
else
  echo "Warning: Source spec file not found at $SOURCE_SPEC_FILE, assuming it is already in ~/rpmbuild/SPECS/"
fi

# Copy Apache mandatory compliance files to rpmbuild/SOURCES
echo "Copying compliance files from $PROJECT_ROOT to ~/rpmbuild/SOURCES..."
for f in LICENSE NOTICE DISCLAIMER; do
    if [ -f "$PROJECT_ROOT/$f" ]; then
        cp -af "$PROJECT_ROOT/$f" ~/rpmbuild/SOURCES/
    else
        echo "Warning: $f not found in $PROJECT_ROOT"
    fi
done

if [ -d "$PROJECT_ROOT/licenses" ]; then
    cp -af "$PROJECT_ROOT/licenses" ~/rpmbuild/SOURCES/
else
    echo "Warning: licenses directory not found in $PROJECT_ROOT"
fi

# Check if the spec file exists at the target location before proceeding
if [ ! -f "$SPEC_FILE" ]; then
  echo "Error: Spec file not found at $SPEC_FILE."
  exit 1
fi

# Build the rpmbuild command based on options
RPMBUILD_CMD="rpmbuild -bb \"$SPEC_FILE\" --define \"version $VERSION\" --define \"release $RELEASE\""
if [ "$DEBUG_BUILD" = true ]; then
    RPMBUILD_CMD+=" --with debug"
fi

# Dry-run mode
if [ "${DRY_RUN:-false}" = true ]; then
  echo "Dry-run mode: This is what would be done:"
  echo "  $RPMBUILD_CMD"
  exit 0
fi

# Run rpmbuild with the provided options
echo "Building RPM with Version: $VERSION, Release: $RELEASE$([ "$DEBUG_BUILD" = true ] && echo ", Debug: enabled")..."
if ! eval "$RPMBUILD_CMD"; then
  echo "Error: rpmbuild failed."
  exit 1
fi

# Print completion message
echo "RPM build completed successfully with Version: $VERSION, Release: $RELEASE"
