# Pipelines

## Naming Prefix Rule

- `PR:<project_name>` for pull-request pipelines
- `COMMIT:<project_name>:<branch_name>` for branch pipelines. It will be executed when a commit committed/merged into the branch.
- `DEV:<your_name>_<project_name>[any_other_info]` for personal development usage. Put your name into the pipeline name so others can know who own it.

## Pipelines for daily work

### PR Pipeline

https://extensions.ci.gpdb.pivotal.io/teams/main/pipelines/PR:diskquota

### Main Branch Pipeline

The development happens on the `gpdb` branch. The commit pipeline for the `gpdb`
https://extensions.ci.gpdb.pivotal.io/teams/main/pipelines/COMMIT:diskquota:gpdb


# Fly a pipeline

## Prerequisite

- Install [ytt](https://carvel.dev/ytt/). It's written in go. So just download the executable for your platform from the [release page](https://github.com/vmware-tanzu/carvel-ytt/releases).
- Make the `fly` command in the `PATH` or export its location to `FLY` env.
- Clone the `gp-continuous-integration` repo to `$HOME/workspace` or set its parent directory to `WORKSPACE` env.
- Login with the `fly` command. Assume we are using `extension` as the target name.

  ```
  fly -t extension login -c https://extensions.ci.gpdb.pivotal.io
  ```
- `cd` to the `concourse` directory.

## Fly the PR pipeline

```
./fly.sh -t extension -c pr
```

## Fly the commit pipeline

```
./fly.sh -t extension -c commit
```

## Fly the release pipeline

TBD

## Fly the dev pipeline

```
./fly.sh -t extension -c dev -p <your_name>_diskquota -b <your_branch>
```

## Webhook

By default, the PR and commit pipelines are using webhook instead of polling to trigger a build. The webhook URL will be printed when flying such a pipeline by `fly.sh`. The webhook needs to be set in the `github repository` -> `Settings` -> `Webhooks` with push notification enabled.

To test if the webhook works, use `curl` to send a `POST` request to the hook URL with some random data. If it is the right URL, the relevant resource will be refreshed on the Concourse UI. The command line looks like:

```
curl --data-raw "foo" <hook_url>
```
