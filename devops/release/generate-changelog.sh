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
# Generate changelog between two git references (tags/commits)
# Usage: ./generate-changelog.sh <from_ref> <to_ref> [repo_owner/repo_name]
#
# Examples:
#   ./generate-changelog.sh 1a40e1f 8178d4f
#   ./generate-changelog.sh v1.0.0 v1.1.0
#   ./generate-changelog.sh 1a40e1f 8178d4f apache/cloudberry
#   ./generate-changelog.sh v1.0.0 v1.1.0 apache/cloudberry
#
# GitHub Token Setup:
# 1. Go to GitHub Settings > Developer settings > Personal access tokens > Tokens (classic)
# 2. Generate new token with 'repo' scope (for private repos) or 'public_repo' scope (for public repos)
# 3. Export the token: export GITHUB_TOKEN=your_token_here

set -e

# Default values
DEFAULT_REPO="apache/cloudberry"
GITHUB_API_BASE="https://api.github.com"

# Function to display usage
usage() {
    echo "Usage: $0 <from_ref> <to_ref> [repo_owner/repo_name]"
    echo ""
    echo "Examples:"
    echo "  $0 1a40e1f 8178d4f"
    echo "  $0 v1.0.0 v1.1.0"
    echo "  $0 1a40e1f 8178d4f apache/cloudberry"
    echo "  $0 v1.0.0 v1.1.0 apache/cloudberry"
    echo ""
    echo "Environment variables:"
    echo "  GITHUB_TOKEN - GitHub personal access token (required)"
    exit 1
}

# Check arguments
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    usage
fi

FROM_REF="$1"
TO_REF="$2"
REPO="${3:-$DEFAULT_REPO}"

# Check if GITHUB_TOKEN is set
if [ -z "$GITHUB_TOKEN" ]; then
    echo "Error: GITHUB_TOKEN environment variable is required"
    echo "Please set it with: export GITHUB_TOKEN=your_token_here"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed"
    echo "Please install jq using your system's package manager"
    exit 1
fi

# Count total commits first
total_commits=$(git log --oneline "$FROM_REF..$TO_REF" | wc -l | tr -d ' ')

echo "Generating changelog for $REPO from $FROM_REF to $TO_REF..."
echo "Found $total_commits commits to process"
echo ""

# Generate changelog
git log --oneline --pretty=format:"%h|%H|%s|%an" "$FROM_REF..$TO_REF" | while IFS='|' read -r short_sha full_sha subject author; do
    # Get PR number for this commit
    pr_number=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
        "$GITHUB_API_BASE/repos/$REPO/commits/$full_sha/pulls" | \
        jq -r '.[0].number // empty')

    if [ -n "$pr_number" ]; then
        echo "* [\`$short_sha\`](https://github.com/$REPO/commit/$full_sha) - $subject ($author) [#$pr_number](https://github.com/$REPO/pull/$pr_number)"
    else
        echo "* [\`$short_sha\`](https://github.com/$REPO/commit/$full_sha) - $subject ($author)"
    fi
done

echo ""
echo "Changelog generation completed!"
