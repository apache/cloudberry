#!/usr/bin/env bash
# ======================================================================
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ======================================================================
#
# cloudberry-release.sh — Apache Cloudberry (Incubating) release utility
#
# This script automates the preparation of an Apache Cloudberry release
# candidate, including version validation, tag creation, and source
# tarball assembly.
#
# Supported Features:
#   - Validates version consistency across configure.ac, configure, gpversion.py, and pom.xml
#   - Supports both final releases and release candidates (e.g., 2.0.0-incubating, 2.0.0-incubating-rc1)
#   - Optionally reuses existing annotated Git tags if they match the current HEAD
#   - Verifies that Git submodules are initialized (if defined in .gitmodules)
#   - Verifies Git identity (user.name and user.email) prior to tagging
#   - Creates a BUILD_NUMBER file (currently hardcoded as 1) in the release tarball
#   - Recursively archives all submodules into the source tarball
#   - Generates SHA-512 checksum (.sha512) using sha512sum for cross-platform consistency
#   - Generates GPG signature (.asc) for the source tarball, unless --skip-signing is used
#   - Moves signed artifacts into a dedicated artifacts/ directory
#   - Verifies integrity and authenticity of artifacts via SHA-512 checksum and GPG signature
#   - Allows skipping of upstream remote URL validation (e.g., for forks) via --skip-remote-check
#   - Excludes macOS extended attribute files (._*, .DS_Store, __MACOSX) for cross-platform compatibility
#   - Validates availability of required tools (sha512sum, gtar, gpg) with platform-specific guidance
#
# Usage:
#   ./cloudberry-release.sh --stage --tag 2.0.0-incubating-rc1 --gpg-user your@apache.org
#
# Options:
#   -s, --stage               Stage a release candidate and generate source tarball
#   -t, --tag <tag>           Tag to apply or validate (e.g., 2.0.0-incubating-rc1)
#   -f, --force-tag-reuse     Allow reuse of an existing tag (must match HEAD)
#   -r, --repo <path>         Optional path to local Cloudberry Git repository
#   -S, --skip-remote-check   Skip validation of remote.origin.url (useful for forks/mirrors)
#   -g, --gpg-user <key>      GPG key ID or email to use for signing (required)
#   -k, --skip-signing        Skip GPG key validation and signature generation
#   -h, --help                Show usage and exit
#
# Requirements:
#   - Must be run from the root of a valid Apache Cloudberry Git clone,
#     or the path must be explicitly provided using --repo
#   - Git user.name and user.email must be configured
#   - Repository remote must be: git@github.com:apache/cloudberry.git
#   - Required tools: sha512sum, tar (gtar on macOS), gpg, xmllint
#   - On macOS: brew install coreutils gnu-tar gnupg
#
# Examples:
#   ./cloudberry-release.sh -s -t 2.0.0-incubating-rc1 --gpg-user your@apache.org
#   ./cloudberry-release.sh -s -t 2.0.0-incubating-rc1 --skip-signing
#   ./cloudberry-release.sh --stage --tag 2.0.0-incubating-rc2 --force-tag-reuse --gpg-user your@apache.org
#   ./cloudberry-release.sh --stage --tag 2.0.0-incubating-rc1 -r ~/cloudberry --skip-remote-check --gpg-user your@apache.org
#
# Notes:
#   - When reusing a tag, the `--force-tag-reuse` flag must be provided.
#   - This script creates a BUILD_NUMBER file in the source root for traceability. It is included in the tarball.
# ======================================================================

set -euo pipefail

# Global variables for detected platform and tools
DETECTED_PLATFORM=""
DETECTED_SHA_TOOL=""
DETECTED_TAR_TOOL=""

# Platform detection and tool check
check_platform_and_tools() {
  local has_errors=false
  
  # Detect platform
  case "$(uname -s)" in
    Linux*)   DETECTED_PLATFORM="Linux" ;;
    Darwin*)  DETECTED_PLATFORM="macOS" ;;
    CYGWIN*|MINGW*|MSYS*) DETECTED_PLATFORM="Windows" ;;
    *)        DETECTED_PLATFORM="Unknown" ;;
  esac
  
  echo "Platform detected: $DETECTED_PLATFORM"
  echo
  
  # Check sha512sum
  if command -v sha512sum >/dev/null 2>&1; then
    DETECTED_SHA_TOOL="sha512sum"
    echo "[OK] SHA-512 tool: $DETECTED_SHA_TOOL"
  else
    echo "[ERROR] SHA-512 tool: sha512sum not found"
    has_errors=true
  fi
  
  # Check tar tool
  if [[ "$DETECTED_PLATFORM" == "macOS" ]]; then
    if command -v gtar >/dev/null 2>&1; then
      DETECTED_TAR_TOOL="gtar"
      echo "[OK] Tar tool: $DETECTED_TAR_TOOL (GNU tar)"
    else
      echo "[ERROR] Tar tool: gtar not found (GNU tar required on macOS)"
      has_errors=true
    fi
  else
    if command -v tar >/dev/null 2>&1; then
      DETECTED_TAR_TOOL="tar"
      echo "[OK] Tar tool: $DETECTED_TAR_TOOL"
    else
      echo "[ERROR] Tar tool: tar not found"
      has_errors=true
    fi
  fi
  
  # Check GPG tool (only when signing is required)
  if [[ "$SKIP_SIGNING" == true ]]; then
    echo "- GPG tool: skipped (--skip-signing enabled)"
  else
    if command -v gpg >/dev/null 2>&1; then
      local gpg_version=$(gpg --version | head -n1 | sed 's/gpg (GnuPG) //')
      echo "[OK] GPG tool: gpg $gpg_version"
    else
      echo "[ERROR] GPG tool: gpg not found"
      has_errors=true
    fi
  fi
  
  # Check xmllint tool
  if command -v xmllint >/dev/null 2>&1; then
    echo "[OK] XML tool: xmllint"
  else
    echo "[ERROR] XML tool: xmllint not found"
    has_errors=true
  fi
  
  # Show installation guidance if there are errors
  if [[ "$has_errors" == true ]]; then
    echo
    echo "Missing required tools. Installation guidance:"
    case "$DETECTED_PLATFORM" in
      Linux)
        echo "  Please install required packages: coreutils tar gnupg libxml2-utils"
        ;;
      macOS)
        echo "  brew install coreutils gnu-tar gnupg"
        ;;
      Windows)
        echo "  Please use Git Bash or install GNU tools"
        ;;
      *)
        echo "  Please install GNU coreutils, tar, GnuPG, and libxml2"
        ;;
    esac
    echo
    echo "These tools ensure consistent cross-platform behavior and secure signing."
    return 1
  fi
  
  return 0
}

confirm() {
  read -r -p "$1 [y/N] " response
  case "$response" in
    [yY][eE][sS]|[yY]) true ;;
    *) echo "Aborted."; exit 1 ;;
  esac
}

# Interactive step confirmation
confirm_next_step() {
  echo
  read -r -p "Press Enter or type y/yes to continue, or 'n' to exit: " response
  case "$response" in
    ""|[yY][eE][sS]|[yY]) 
      return 0 
      ;;
    [nN]|[nN][oO]) 
      echo "Process stopped by user."
      exit 0 
      ;;
    *) 
      echo "Invalid input. Please press Enter or type y/yes to continue, or 'n' to exit."
      confirm_next_step
      ;;
  esac
}

section() {
  echo
  echo "================================================================="
  echo ">> $1"
  echo "================================================================="
}

show_help() {
  echo "Apache Cloudberry (Incubating) Release Tool"
  echo
  echo "Usage:"
  echo "  $0 --stage --tag <version-tag>"
  echo
  echo "Options:"
  echo "  -s, --stage"
  echo "      Stage a release candidate and generate source tarball"
  echo
  echo "  -t, --tag <tag>"
  echo "      Required with --stage (e.g., 2.0.0-incubating-rc1)"
  echo
  echo "  -f, --force-tag-reuse"
  echo "      Reuse existing tag if it matches current HEAD"
  echo
  echo "  -r, --repo <path>"
  echo "      Optional path to a local Cloudberry Git repository clone"
  echo
  echo "  -S, --skip-remote-check"
  echo "      Skip remote.origin.url check (use for forks or mirrors)"
  echo "      Required for official releases:"
  echo "        git@github.com:apache/cloudberry.git"
  echo
  echo "  -g, --gpg-user <key>"
  echo "      GPG key ID or email to use for signing (required unless --skip-signing)"
  echo
  echo "  -k, --skip-signing"
  echo "      Skip GPG key validation and signature generation"
  echo
  echo "  -h, --help"
  echo "      Show this help message"
  exit 1
}

# Flags
STAGE=false
SKIP_SIGNING=false
TAG=""
FORCE_TAG_REUSE=false
REPO_ARG=""
SKIP_REMOTE_CHECK=false
GPG_USER=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -g|--gpg-user)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --gpg-user requires an email." >&2
        show_help
      fi
      GPG_USER="$2"
      shift 2
      ;;
    -s|--stage)
      STAGE=true
      shift
      ;;
    -t|--tag)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: Missing tag value after --tag" >&2
        show_help
      fi
      TAG="$2"
      shift 2
      ;;
    -f|--force-tag-reuse)
      FORCE_TAG_REUSE=true
      shift
      ;;
    -r|--repo)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --repo requires a path." >&2
        show_help
      fi
      REPO_ARG="$2"
      shift 2
      ;;
    -S|--skip-remote-check)
      SKIP_REMOTE_CHECK=true
      shift
      ;;
    -k|--skip-signing)
      SKIP_SIGNING=true
      shift
      ;;
    -h|--help)
      show_help
      ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      show_help
      ;;
  esac
done

# GPG signing checks
if [[ "$SKIP_SIGNING" != true ]]; then
  if [[ -z "$GPG_USER" ]]; then
    echo "ERROR: --gpg-user is required for signing the release tarball." >&2
    show_help
  fi

  if ! gpg --list-keys "$GPG_USER" > /dev/null 2>&1; then
    echo "ERROR: GPG key '$GPG_USER' not found in your local keyring." >&2
    echo "Please import or generate the key before proceeding." >&2
    exit 1
  fi
else
  echo "INFO: GPG signing has been intentionally skipped (--skip-signing)."
fi

if [[ -n "$REPO_ARG" ]]; then
  # Validate the specified repository path
  if [[ ! -d "$REPO_ARG" || ! -f "$REPO_ARG/configure.ac" ]]; then
    echo "ERROR: '$REPO_ARG' does not appear to be a valid Cloudberry source directory."
    echo "Expected to find a 'configure.ac' file but it is missing."
    echo
    echo "Hint: Make sure you passed the correct --repo path to a valid Git clone."
    exit 1
  fi
  
  # Change to the specified repository directory
  cd "$REPO_ARG"

  if [[ ! -d ".git" ]]; then
    echo "ERROR: '$REPO_ARG' is not a valid Git repository."
    exit 1
  fi

  if [[ "$SKIP_REMOTE_CHECK" != true ]]; then
    REMOTE_URL=$(git config --get remote.origin.url || true)
    if [[ "$REMOTE_URL" != "git@github.com:apache/cloudberry.git" ]]; then
      echo "ERROR: remote.origin.url must be set to 'git@github.com:apache/cloudberry.git' for official releases."
      echo "  Found: '${REMOTE_URL:-<unset>}'"
      echo
      echo "This check ensures the release is being staged from the authoritative upstream repository."
      echo "Use --skip-remote-check only if this is a fork or non-release automation."
      exit 1
    fi
  fi
fi

# If --repo was not provided, ensure we are in a valid source directory
if [[ -z "$REPO_ARG" ]]; then
  if [[ ! -f configure.ac || ! -f gpMgmt/bin/gppylib/gpversion.py || ! -f pom.xml ]]; then
    echo "ERROR: You must run this script from the root of a valid Cloudberry Git clone"
    echo "       or pass the path using --repo <source-dir>."
    echo
    echo "Missing one or more expected files:"
    echo "  - configure.ac"
    echo "  - gpMgmt/bin/gppylib/gpversion.py"
    echo "  - pom.xml"
    exit 1
  fi
fi

if ! $STAGE && [[ -z "$TAG" ]]; then
  show_help
fi

if $STAGE && [[ -z "$TAG" ]]; then
  echo "ERROR: --tag (-t) is required when using --stage." >&2
  show_help
fi

# Check platform and required tools early
if $STAGE; then
  section "Platform and Tool Detection"
  if ! check_platform_and_tools; then
    exit 1
  fi
  confirm_next_step
fi

section "Validating Version Consistency"

# Extract version from configure.ac
CONFIGURE_AC_VERSION=$(grep "^AC_INIT" configure.ac | sed -E "s/^AC_INIT\(\[[^]]+\], \[([^]]+)\].*/\1/")
CONFIGURE_AC_MAJOR=$(echo "$CONFIGURE_AC_VERSION" | cut -d. -f1)
EXPECTED="[$CONFIGURE_AC_MAJOR,99]"

# Validate tag format
SEMVER_REGEX="^${CONFIGURE_AC_MAJOR}\\.[0-9]+\\.[0-9]+(-incubating(-rc[0-9]+)?)?$"
if ! [[ "$TAG" =~ $SEMVER_REGEX ]]; then
  echo "ERROR: Tag '$TAG' does not match expected pattern for major version $CONFIGURE_AC_MAJOR (e.g., ${CONFIGURE_AC_MAJOR}.0.0-incubating or ${CONFIGURE_AC_MAJOR}.0.0-incubating-rc1)"
  exit 1
fi

# Extract base version from tag (remove -rc suffix if present)
BASE_VERSION="$TAG"
if [[ "$TAG" =~ ^(.+)-rc[0-9]+$ ]]; then
  BASE_VERSION="${BASH_REMATCH[1]}"
fi

echo "Version validation strategy:"
echo "  Tag: $TAG"
echo "  Base version (for source files): $BASE_VERSION"

# Check gpversion.py consistency
PY_LINE=$(grep "^MAIN_VERSION" gpMgmt/bin/gppylib/gpversion.py | sed -E 's/#.*//' | tr -d '[:space:]')

if [[ "$PY_LINE" != "MAIN_VERSION=$EXPECTED" ]]; then
  echo "ERROR: gpversion.py MAIN_VERSION is $PY_LINE, but configure.ac suggests $EXPECTED"
  echo "Please correct this mismatch before proceeding."
  exit 1
fi

# Ensure configure.ac version matches base version (without -rc suffix)
if [[ "$CONFIGURE_AC_VERSION" != "$BASE_VERSION" ]]; then
  echo "ERROR: configure.ac version ($CONFIGURE_AC_VERSION) does not match base version ($BASE_VERSION)"
  echo "For RC tags like '$TAG', configure.ac should contain the base version '$BASE_VERSION'"
  exit 1
fi

# Ensure the generated 'configure' script matches base version
CONFIGURE_VERSION_LINE=$(grep "^PACKAGE_VERSION=" configure || true)
CONFIGURE_VERSION=$(echo "$CONFIGURE_VERSION_LINE" | sed -E "s/^PACKAGE_VERSION='([^']+)'.*/\1/")

if [[ "$CONFIGURE_VERSION" != "$BASE_VERSION" ]]; then
  echo "ERROR: Version in generated 'configure' script ($CONFIGURE_VERSION) does not match base version ($BASE_VERSION)."
  echo "This likely means autoconf was not run after updating configure.ac to the base version."
  exit 1
fi



# Extract version from pom.xml using xmllint with namespace stripping
POM_VERSION=$(xmllint --xpath '//*[local-name()="project"]/*[local-name()="version"]/text()' pom.xml 2>/dev/null || true)

if [[ -z "$POM_VERSION" ]]; then
  echo "ERROR: Could not extract <version> from pom.xml"
  exit 1
fi

if [[ "$POM_VERSION" != "$BASE_VERSION" ]]; then
  echo "ERROR: Version in pom.xml ($POM_VERSION) does not match base version ($BASE_VERSION)."
  echo "For RC tags like '$TAG', pom.xml should contain the base version '$BASE_VERSION'"
  exit 1
fi

# Ensure working tree is clean
if ! git diff-index --quiet HEAD --; then
  echo "ERROR: Working tree is not clean. Please commit or stash changes before proceeding."
  exit 1
fi

echo "Version consistency verified"
printf "    %-14s: %s\n" "Release Tag"   "$TAG"
printf "    %-14s: %s\n" "Base Version"  "$BASE_VERSION"
printf "    %-14s: %s\n" "configure.ac"  "$CONFIGURE_AC_VERSION"
printf "    %-14s: %s\n" "configure"     "$CONFIGURE_VERSION"
printf "    %-14s: %s\n" "pom.xml"       "$POM_VERSION"
printf "    %-14s: %s\n" "gpversion.py"  "${EXPECTED//[\[\]]}"
confirm_next_step

section "Checking the state of the Tag"

# Check if the tag already exists before making any changes
if git rev-parse "$TAG" >/dev/null 2>&1; then
  TAG_COMMIT=$(git rev-list -n 1 "$TAG")
  HEAD_COMMIT=$(git rev-parse HEAD)

  if [[ "$TAG_COMMIT" == "$HEAD_COMMIT" && "$FORCE_TAG_REUSE" == true ]]; then
    echo "INFO: Tag '$TAG' already exists and matches HEAD. Proceeding with reuse."
  elif [[ "$FORCE_TAG_REUSE" == true ]]; then
    echo "ERROR: --force-tag-reuse was specified but tag '$TAG' does not match HEAD."
    echo "       Tags must be immutable. Cannot continue."
    exit 1
  else
    echo "ERROR: Tag '$TAG' already exists and does not match HEAD."
    echo "       Use --force-tag-reuse only when HEAD matches the tag commit."
    exit 1
  fi
elif [[ "$FORCE_TAG_REUSE" == true ]]; then
  echo "ERROR: --force-tag-reuse was specified, but tag '$TAG' does not exist."
  echo "       You can only reuse a tag if it already exists."
  exit 1
else
  echo "INFO: Tag '$TAG' does not yet exist. It will be created during staging."
fi

confirm_next_step

# Check and display submodule initialization status
if [ -s .gitmodules ]; then
  section "Checking Git Submodules"

  UNINITIALIZED=false
  while read -r status path rest; do
    if [[ "$status" == "-"* ]]; then
      echo "Uninitialized: $path"
      UNINITIALIZED=true
    else
      echo "Initialized  : $path"
    fi
  done < <(git submodule status)

  if [[ "$UNINITIALIZED" == true ]]; then
    echo
    echo "ERROR: One or more Git submodules are not initialized."
    echo "Please run:"
    echo "  git submodule update --init --recursive"
    echo "before proceeding with the release preparation."
    exit 1
  fi
fi

section "Checking GIT_USER_NAME and GIT_USER_EMAIL values"

if $STAGE; then
  # Validate Git environment before performing tag operation
  GIT_USER_NAME=$(git config --get user.name || true)
  GIT_USER_EMAIL=$(git config --get user.email || true)

  echo "Git User Info:"
  printf "    %-14s: %s\n" "user.name"  "${GIT_USER_NAME:-<unset>}"
  printf "    %-14s: %s\n" "user.email" "${GIT_USER_EMAIL:-<unset>}"

  if [[ -z "$GIT_USER_NAME" || -z "$GIT_USER_EMAIL" ]]; then
    echo "ERROR: Git configuration is incomplete."
    echo
    echo "  Detected:"
    echo "    user.name  = ${GIT_USER_NAME:-<unset>}"
    echo "    user.email = ${GIT_USER_EMAIL:-<unset>}"
    echo
    echo "  Git requires both to be set in order to create annotated tags for releases."
    echo "  You may configure them globally using:"
    echo "    git config --global user.name \"Your Name\""
    echo "    git config --global user.email \"your@apache.org\""
    echo
    echo "  Alternatively, set them just for this repo using the same commands without --global."
    exit 1
  fi

section "Staging release: $TAG"

  if [[ "$FORCE_TAG_REUSE" == false ]]; then
    confirm "You are about to create tag '$TAG'. Continue?"
    git tag -a "$TAG" -m "Apache Cloudberry (Incubating) ${TAG} Release Candidate"
  else
    echo "INFO: Reusing existing tag '$TAG'; skipping tag creation."
  fi

  echo "Creating BUILD_NUMBER file with commit hash"
  build_num=$(git rev-parse --short HEAD)
  echo "$build_num" > BUILD_NUMBER

  echo -e "\nTag Summary"
  TAG_OBJECT=$(git rev-parse "$TAG")
  TAG_COMMIT=$(git rev-list -n 1 "$TAG")
  echo "$TAG (tag object): $TAG_OBJECT"
  echo "    Points to commit: $TAG_COMMIT"
  git log -1 --format="%C(auto)%h %d" "$TAG"
  confirm_next_step

  section "Creating Source Tarball"

  TAR_NAME="apache-cloudberry-${TAG}-src.tar.gz"
  TMP_DIR=$(mktemp -d)
  trap 'rm -rf "$TMP_DIR"' EXIT

  # Set environment variables to prevent macOS extended attributes
  export COPYFILE_DISABLE=1
  export COPY_EXTENDED_ATTRIBUTES_DISABLE=1

  git archive --format=tar --prefix="apache-cloudberry-${TAG}/" "$TAG" | tar -x -C "$TMP_DIR"
  cp BUILD_NUMBER "$TMP_DIR/apache-cloudberry-${TAG}/"

  # Archive submodules if any
  if [ -s .gitmodules ]; then
    git submodule foreach --recursive --quiet "
      echo \"Archiving submodule: \$sm_path\"
      fullpath=\"\$toplevel/\$sm_path\"
      destpath=\"$TMP_DIR/apache-cloudberry-$TAG/\$sm_path\"
      mkdir -p \"\$destpath\"
      git -C \"\$fullpath\" archive --format=tar --prefix=\"\$sm_path/\" HEAD | tar -x -C \"$TMP_DIR/apache-cloudberry-$TAG\"
    "
  fi

  # Clean up macOS extended attributes if on macOS
  if [[ "$DETECTED_PLATFORM" == "macOS" ]]; then
    echo "Cleaning macOS extended attributes from extracted files..."
    # Remove all extended attributes recursively
    if command -v xattr >/dev/null 2>&1; then
      find "$TMP_DIR/apache-cloudberry-${TAG}" -type f -exec xattr -c {} \; 2>/dev/null || true
      echo "[OK] Extended attributes cleaned using xattr"
    fi
    
    # Remove any ._* files that might have been created
    find "$TMP_DIR/apache-cloudberry-${TAG}" -name '._*' -delete 2>/dev/null || true
    find "$TMP_DIR/apache-cloudberry-${TAG}" -name '.DS_Store' -delete 2>/dev/null || true
    find "$TMP_DIR/apache-cloudberry-${TAG}" -name '__MACOSX' -type d -exec rm -rf {} \; 2>/dev/null || true
    echo "[OK] macOS-specific files removed"
  fi

  # Create tarball using the detected tar tool
  if [[ "$DETECTED_PLATFORM" == "macOS" ]]; then
    echo "Using GNU tar for cross-platform compatibility..."
    $DETECTED_TAR_TOOL --exclude='._*' --exclude='.DS_Store' --exclude='__MACOSX' -czf "$TAR_NAME" -C "$TMP_DIR" "apache-cloudberry-${TAG}"
    echo "INFO: macOS detected - applied extended attribute cleanup and GNU tar"
  else
    # On other platforms, use standard tar
    $DETECTED_TAR_TOOL -czf "$TAR_NAME" -C "$TMP_DIR" "apache-cloudberry-${TAG}"
  fi
  
  rm -rf "$TMP_DIR"
  echo -e "Archive saved to: $TAR_NAME"
  
  # Verify that no macOS extended attribute files are included
  if [[ "$DETECTED_PLATFORM" == "macOS" ]]; then
    echo "Verifying tarball does not contain macOS-specific files..."
    MACOS_FILES=$($DETECTED_TAR_TOOL -tzf "$TAR_NAME" | grep -E '\._|\.DS_Store|__MACOSX' || true)
    if [[ -n "$MACOS_FILES" ]]; then
      echo "WARNING: Found macOS-specific files in tarball:"
      echo "$MACOS_FILES"
      echo "This may cause compilation issues on Linux systems."
    else
      echo "[OK] Tarball verified clean of macOS-specific files"
    fi
    
    # Additional check for extended attributes in tar headers
    echo "Checking for extended attribute headers in tarball..."
    if $DETECTED_TAR_TOOL -tvf "$TAR_NAME" 2>&1 | grep -q "LIBARCHIVE.xattr" 2>/dev/null; then
      echo "WARNING: Tarball may still contain extended attribute headers"
      echo "This could cause 'Ignoring unknown extended header keyword' warnings on Linux"
    else
      echo "[OK] No extended attribute headers detected in tarball (GNU tar used)"
    fi
  fi
  
  confirm_next_step

  # Generate SHA-512 checksum
  section "Generating SHA-512 Checksum"

  echo -e "\nGenerating SHA-512 checksum"
  sha512sum "$TAR_NAME" > "${TAR_NAME}.sha512"
  echo "Checksum saved to: ${TAR_NAME}.sha512"
  confirm_next_step

  section "Signing with GPG key: $GPG_USER"
  # Conditionally generate GPG signature
  if [[ "$SKIP_SIGNING" != true ]]; then
    echo -e "\nSigning tarball with GPG key: $GPG_USER"
    gpg --armor --detach-sign --local-user "$GPG_USER" "$TAR_NAME"
    echo "GPG signature saved to: ${TAR_NAME}.asc"
  else
    echo "INFO: Skipping tarball signing as requested (--skip-signing)"
  fi

  # Move artifacts to top-level artifacts directory
  # At this point, we're always in the cloudberry repository directory
  # (either we started there, or we cd'd there via --repo)
  ARTIFACTS_DIR="$(cd .. && pwd)/artifacts"
  
  mkdir -p "$ARTIFACTS_DIR"

  section "Moving Artifacts to $ARTIFACTS_DIR"

  echo -e "\nMoving release artifacts to: $ARTIFACTS_DIR"
  mv -vf "$TAR_NAME" "$ARTIFACTS_DIR/"
  mv -vf "${TAR_NAME}.sha512" "$ARTIFACTS_DIR/"
  [[ -f "${TAR_NAME}.asc" ]] && mv -vf "${TAR_NAME}.asc" "$ARTIFACTS_DIR/"
  confirm_next_step

  section "Verifying sha512 ($ARTIFACTS_DIR/${TAR_NAME}.sha512) Release Artifact"
  (cd "$ARTIFACTS_DIR" && sha512sum -c "${TAR_NAME}.sha512")
  confirm_next_step

  section "Verifying GPG Signature ($ARTIFACTS_DIR/${TAR_NAME}.asc) Release Artifact"

  if [[ "$SKIP_SIGNING" != true ]]; then
    gpg --verify "$ARTIFACTS_DIR/${TAR_NAME}.asc" "$ARTIFACTS_DIR/$TAR_NAME"
  else
    echo "INFO: Signature verification skipped (--skip-signing). Signature is only available when generated via this script."
  fi
  confirm_next_step

  section "Release candidate for $TAG staged successfully"
fi
