<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# GitHub Actions Workflows

This directory contains GitHub Actions workflows for Apache Cloudberry CI/CD.

## Table of Contents

- [Available Workflows](#available-workflows)
- [Manual Workflow Triggers](#manual-workflow-triggers)
- [Artifact Reuse for Faster Testing](#artifact-reuse-for-faster-testing)
- [Running Workflows in Forked Repositories](#running-workflows-in-forked-repositories)

## Available Workflows

| Workflow | Purpose | Trigger |
|----------|---------|---------|
| `build-cloudberry.yml` | Main CI: build, test, create RPMs | Push, PR, Manual |
| `build-dbg-cloudberry.yml` | Debug build with assertions enabled | Push, PR, Manual |
| `apache-rat-audit.yml` | License header compliance check | Push, PR |
| `coverity.yml` | Static code analysis with Coverity | Weekly, Manual |
| `sonarqube.yml` | Code quality analysis with SonarQube | Push to main |
| `docker-cbdb-build-containers.yml` | Build Docker images for CI | Manual |
| `docker-cbdb-test-containers.yml` | Build test Docker images | Manual |

## Manual Workflow Triggers

Many workflows support manual triggering via `workflow_dispatch`, allowing developers to run CI jobs on-demand.

### How to Manually Trigger a Workflow

1. Navigate to the **Actions** tab in GitHub
2. Select the workflow from the left sidebar (e.g., "Build and Test Cloudberry")
3. Click **Run workflow** button (top right)
4. Select your branch
5. Configure input parameters (if available)
6. Click **Run workflow**

### Workflow Input Parameters

#### `build-cloudberry.yml` - Main CI

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `test_selection` | Comma-separated list of tests to run, or "all" | `all` | `ic-good-opt-off,ic-contrib` |
| `reuse_artifacts_from_run_id` | Run ID to reuse build artifacts from (see below) | _(empty)_ | `12345678901` |

**Available test selections:**
- `all` - Run all test suites
- `ic-good-opt-off` - Installcheck with optimizer off
- `ic-good-opt-on` - Installcheck with optimizer on
- `ic-contrib` - Contrib extension tests
- `ic-resgroup` - Resource group tests
- `ic-resgroup-v2` - Resource group v2 tests
- `ic-resgroup-v2-memory-accounting` - Resource group memory tests
- `ic-singlenode` - Single-node mode tests
- `make-installcheck-world` - Full test suite
- And more... (see workflow for complete list)

## Artifact Reuse for Faster Testing

When debugging test failures, rebuilding Cloudberry (~50-70 minutes) on every iteration is inefficient. The artifact reuse feature allows you to reuse build artifacts from a previous successful run.

### How It Works

1. Build artifacts (RPMs, source tarballs) from a previous workflow run are downloaded
2. Build job is skipped (saves ~45-60 minutes)
3. RPM installation test is skipped (saves ~5-10 minutes)
4. Test jobs run with the reused artifacts
5. You can iterate on test configurations without rebuilding

### Step-by-Step Guide

#### 1. Find the Run ID

After a successful build (even if tests failed), get the run ID:

**Option A: From GitHub Actions UI**
- Go to **Actions** tab → Click on a completed workflow run
- The URL will be: `https://github.com/apache/cloudberry/actions/runs/12345678901`
- The run ID is `12345678901`

**Option B: From GitHub API**
```bash
# List recent workflow runs
gh run list --workflow=build-cloudberry.yml --limit 5

# Get run ID from specific branch
gh run list --workflow=build-cloudberry.yml --branch=my-feature --limit 1
```

#### 2. Trigger New Run with Artifact Reuse

**Via GitHub UI:**
1. Go to **Actions** → **Build and Test Cloudberry**
2. Click **Run workflow**
3. Enter the run ID in **"Reuse build artifacts from a previous run ID"**
4. Optionally customize **test_selection**
5. Click **Run workflow**

**Via GitHub CLI:**
```bash
# Reuse artifacts from run 12345678901, run only specific tests
gh workflow run build-cloudberry.yml \
  --field reuse_artifacts_from_run_id=12345678901 \
  --field test_selection=ic-good-opt-off
```

#### 3. Monitor Test Execution

- Build job will be skipped (shows as "Skipped" in Actions UI)
- RPM Install Test will be skipped
- Test jobs will run with artifacts from the specified run ID
- Total time: ~15-30 minutes (vs ~65-100 minutes for full build+test)

### Use Cases

**Debugging a specific test failure:**
```bash
# Run 1: Full build + all tests (finds test failure in ic-good-opt-off)
gh workflow run build-cloudberry.yml

# Get the run ID from output
RUN_ID=$(gh run list --workflow=build-cloudberry.yml --limit 1 --json databaseId --jq '.[0].databaseId')

# Run 2: Reuse artifacts, run only failing test
gh workflow run build-cloudberry.yml \
  --field reuse_artifacts_from_run_id=$RUN_ID \
  --field test_selection=ic-good-opt-off
```

**Testing different configurations:**
```bash
# Test with optimizer off, then on, using same build
gh workflow run build-cloudberry.yml \
  --field reuse_artifacts_from_run_id=$RUN_ID \
  --field test_selection=ic-good-opt-off

gh workflow run build-cloudberry.yml \
  --field reuse_artifacts_from_run_id=$RUN_ID \
  --field test_selection=ic-good-opt-on
```

### Limitations

- Artifacts expire after 90 days (GitHub default retention)
- Run ID must be from the same repository (or accessible fork)
- Artifacts must include both RPM and source build artifacts
- Cannot reuse artifacts across different OS/architecture combinations
- Changes to source code require a fresh build

## Running Workflows in Forked Repositories

GitHub Actions workflows are enabled in forks, allowing you to validate changes before submitting a Pull Request.

### Initial Setup (One-Time)

1. **Fork the repository** to your GitHub account

2. **Enable GitHub Actions** in your fork:
   - Go to your fork's **Actions** tab
   - Click **"I understand my workflows, go ahead and enable them"**

**Secrets Configuration:**

No manual secret configuration is required for the main build and test workflows.

- `GITHUB_TOKEN` is automatically provided by GitHub and used when downloading artifacts from previous runs (artifact reuse feature)
- DockerHub secrets (`DOCKERHUB_USER`, `DOCKERHUB_TOKEN`) are only required for building custom container images (advanced/maintainer use case, not needed for typical development)

### Workflow Behavior in Forks

- ✅ **Automated triggers work**: Push and PR events trigger workflows
- ✅ **Manual triggers work**: `workflow_dispatch` is fully functional
- ✅ **Artifact reuse works**: Can reuse artifacts from previous runs in your fork
- ⚠️ **Cross-fork artifact reuse**: Not supported (security restriction)
- ⚠️ **Some features may be limited**: Certain features requiring organization-level secrets may not work

### Best Practices for Fork Development

1. **Test locally first** when possible (faster iteration)
2. **Use manual triggers** to avoid burning GitHub Actions minutes unnecessarily
3. **Use artifact reuse** to iterate on test failures efficiently
4. **Push to feature branches** to trigger automated CI
5. **Review Actions tab** to ensure workflows completed successfully before opening PR

### Example Fork Workflow

```bash
# 1. Create feature branch in fork
git checkout -b fix-test-failure

# 2. Make changes and push to fork
git commit -am "Fix test failure"
git push origin fix-test-failure

# 3. CI runs automatically on push

# 4. If tests fail, iterate using artifact reuse
# Get run ID from your fork's Actions tab
gh workflow run build-cloudberry.yml \
  --field reuse_artifacts_from_run_id=12345678901 \
  --field test_selection=ic-good-opt-off

# 5. Once tests pass, open PR to upstream
gh pr create --web
```

## Troubleshooting

### "Build job was skipped but tests failed to start"

**Cause:** Artifacts from specified run ID not found or expired

**Solution:**
- Verify the run ID is correct
- Check that run completed successfully (built artifacts)
- Run a fresh build if artifacts expired (>90 days)

### "Workflow not found in fork"

**Cause:** GitHub Actions not enabled in fork

**Solution:**
- Go to fork's **Actions** tab
- Click to enable workflows

### "Resource not accessible by integration"

**Cause:** Workflow trying to access artifacts from different repository

**Solution:**
- Can only reuse artifacts from same repository
- Run a fresh build in your fork first, then reuse those artifacts

## Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Cloudberry Contributing Guide](../../CONTRIBUTING.md)
- [Cloudberry Build Guide](../../deploy/build/README.md)
- [DevOps Scripts](../../devops/README.md)
