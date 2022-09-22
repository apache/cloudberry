# Pipelines

## Naming Prefix Rule

- `pr.<project_name>` for pull-request pipelines
- `merge.<project_name>.<branch_name>` for branch pipelines. It will be executed when a commit committed/merged into the branch.
- `dev.<project_name>.<branch_name>.<your_postfix>` for personal development usage. Put your name into the pipeline name so others can know who own it.
- `<pipeline>_test.<project_name>.<branch_name>` for pipeline debugging.

## Pipelines for daily work

### PR Pipeline

https://dev2.ci.gpdb.pivotal.io/teams/gp-extensions/pipelines/pr.diskquota

### Main Branch Pipeline

The development happens on the `gpdb` branch. The merge pipeline for the `gpdb` branch is
https://dev2.ci.gpdb.pivotal.io/teams/gp-extensions/pipelines/merge.diskquota:gpdb


# Fly a pipeline

## Prerequisite

- Install [ytt](https://carvel.dev/ytt/). It's written in go. So just download the executable for your platform from the [release page](https://github.com/vmware-tanzu/carvel-ytt/releases).
- Make the `fly` command in the `PATH` or export its location to `FLY` env.
- Login with the `fly` command. Assume we are using `dev2` as the target name.

```
# -n gp-extensions is to set the concourse team
fly -t dev2 login -c https://dev2.ci.gpdb.pivotal.io -n gp-extensions
```

- `cd` to the `concourse` directory.

## Fly the PR pipeline

```
./fly.sh -t dev2 -c pr
```

## Fly the merge pipeline

```
./fly.sh -t dev2 -c merge
```

## Fly the release pipeline

By default, the release is built from the `gpdb` branch.

The release pipeline should be located in https://prod.ci.gpdb.pivotal.io

```
# Login to prod
fly -t prod login -c https://prod.ci.gpdb.pivotal.io
# Fly the release pipeline
./fly.sh -t prod -c rel
```

To fly a release pipeline from a specific branch:

```
./fly.sh -t <target> -c rel -b release/<major>.<minor>
```

## Fly the dev pipeline

```
./fly.sh -t dev2 -c dev -p <your_postfix> -b <your_branch>
```

## Webhook

By default, the PR and merge pipelines are using webhook instead of polling to trigger a build. The webhook URL will be printed when flying such a pipeline by `fly.sh`. The webhook needs to be set in the `github repository` -> `Settings` -> `Webhooks` with push notification enabled.

To test if the webhook works, use `curl` to send a `POST` request to the hook URL with some random data. If it is the right URL, the relevant resource will be refreshed on the Concourse UI. The command line looks like:

```
curl --data-raw "foo" <hook_url>
```

# FAQ

## PR pipeline is not triggered.

The PR pipeline relies on the webhook to detect the new PR. However, due to the the limitation of the webhook implemention of concourse, we rely on the push hook for this. It means if the PR is from a forked repo, the PR pipeline won't be triggered immediately. To manually trigger the pipeline, go to https://dev2.ci.gpdb.pivotal.io/teams/gp-extensions/pipelines/pr.diskquota/resources/diskquota_pr and click ⟳ button there.

TIPS: Just don't fork, name your branch as `<your_id>/<branch_name>` and push it here to create PR.
