#!/bin/bash

set -e

workspace=${WORKSPACE:-"$HOME/workspace"}
fly=${FLY:-"fly"}
echo "'workspace' location: ${workspace}"
echo "'fly' command: ${fly}"
echo ""

usage() {
    echo "Usage: $0 -t <concourse_target> -c <pr|commit|dev> [-p <pipeline_name>] [-b branch]" 1>&2
    if [ -n "$1" ]; then
        echo "$1"
    fi
    exit 1
}

# Parse command line options
while getopts ":c:t:p:b:" o; do
    case "${o}" in
        c)
            # pipeline type/config. pr/commit/dev/release
            pipeline_config=${OPTARG}
            ;;
        t)
            # concourse target
            target=${OPTARG}
            ;;
        p)
            # pipeline name
            pipeline_name=${OPTARG}
            ;;
        b)
            # branch name
            branch=${OPTARG}
            ;;
        *)
            usage ""
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${target}" ] || [ -z "${pipeline_config}" ]; then
    usage ""
fi

# Decide ytt options to generate pipeline
case ${pipeline_config} in
  pr)
      if [ -z "${pipeline_name}" ]; then
          pipeline_name="PR:diskquota"
      fi
      config_file="pr.yml"
      hook_res="diskquota_pr"
    ;;
  commit)
      if [ -z "${pipeline_name}" ]; then
          pipeline_name="COMMIT:diskquota:gpdb"
      fi
      # Default branch is 'gpdb' as it is our main branch
      if [ -z "${branch}" ]; then
          branch="gpdb"
      fi
      config_file="commit.yml"
      hook_res="diskquota_commit"
    ;;
  dev)
      if [ -z "${pipeline_name}" ]; then
          usage "'-p' needs to be supplied to specify the pipeline name for flying a 'dev' pipeline."
      fi
      pipeline_name="DEV:${pipeline_name}"
      config_file="dev.yml"
    ;;
  *)
      usage ""
    ;;
esac

yml_path="/tmp/diskquota_pipeline.yml"
my_path=$(realpath "${BASH_SOURCE[0]}")
ytt_base=$(dirname "${my_path}")/pipeline

ytt --data-values-file "${ytt_base}/res_def.yml" \
    -f "${ytt_base}/base.lib.yml" \
    -f "${ytt_base}/job_def.lib.yml" \
    -f "${ytt_base}/trigger_def.lib.yml" \
    -f "${ytt_base}/${config_file}" > "${yml_path}"
echo "Generated pipeline yaml '${yml_path}'."

echo ""
echo "Fly the pipeline..."
set -v
"${fly}" \
    -t "${target}" \
    sp \
    -p "${pipeline_name}" \
    -c "${yml_path}" \
    -l "${workspace}/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml" \
    -l "${workspace}/gp-continuous-integration/secrets/gp-extensions-common.yml" \
    -l "${workspace}/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml" \
    -v "diskquota-branch=${branch}"
set +v

if [ "${pipeline_config}" == "dev" ]; then
    exit 0
fi

echo ""
echo "================================================================================"
echo "Remeber to set the the webhook URL on GitHub:"
echo "https://extensions.ci.gpdb.pivotal.io/api/v1/teams/main/pipelines/${pipeline_name}/resources/${hook_res}/check/webhook?webhook_token=<hook_token>"
echo "You may need to change the base URL if a differnt concourse server is used."
echo "================================================================================"
