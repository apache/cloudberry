#!/bin/bash

set -e

fly=${FLY:-"fly"}
echo "'fly' command: ${fly}"
echo ""
proj_name="diskquota"
concourse_team="main"

usage() {
    if [ -n "$1" ]; then
        echo "$1" 1>&2
        echo "" 1>&2
    fi

    echo "Usage: $0 -t <concourse_target> -c <pr|rel|merge|dev> [-p <postfix>] [-b branch] [-T]"
    echo "Options:"
    echo "       '-T' adds '_test' suffix to the pipeline type. Useful for pipeline debugging."
    exit 1
}

# Hacky way to find out which concourse team is being used.
# The team name is needed to generate webhook URL
detect_concourse_team() {
    local target="$1"
    local fly_rc_file="$HOME/.flyrc"
    local found_target=false
    while read -r line;
    do
        line="$(echo -e "${line}" | tr -d '[:space:]')"
        if [ ${found_target} != true ] && [ "${line}" = "${target}:" ]; then
            found_target=true
        fi
        if [ ${found_target} = true ] && [[ "${line}" == team:* ]]; then
            concourse_team=$(echo "${line}" | cut --delimiter=":" --fields=2)
            echo "Use concourse target: ${target}, team: ${concourse_team}"
            return
        fi
    done < "${fly_rc_file}"
}

# Parse command line options
while getopts ":c:t:p:b:T" o; do
    case "${o}" in
        c)
            # pipeline type/config. pr/merge/dev/rel
            pipeline_config=${OPTARG}
            ;;
        t)
            # concourse target
            target=${OPTARG}
            ;;
        p)
            # pipeline name
            postfix=${OPTARG}
            ;;
        b)
            # branch name
            branch=${OPTARG}
            ;;
        T)
            test_suffix="_test"
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

detect_concourse_team "${target}"

pipeline_type=""
# Decide ytt options to generate pipeline
case ${pipeline_config} in
  pr)
      pipeline_type="pr"
      config_file="pr.yml"
      hook_res="${proj_name}_pr"
    ;;
  merge|commit)
      # Default branch is 'gpdb' as it is our main branch
      if [ -z "${branch}" ]; then
          branch="gpdb"
      fi
      pipeline_type="merge"
      config_file="commit.yml"
      hook_res="${proj_name}_commit"
    ;;
  dev)
      if [ -z "${postfix}" ]; then
          usage "'-p' needs to be supplied to specify the pipeline name postfix for flying a 'dev' pipeline."
      fi
      if [ -z "${branch}" ]; then
          usage "'-b' needs to be supplied to specify the branch for flying a 'dev' pipeline."
      fi
      pipeline_type="dev"
      config_file="dev.yml"
    ;;
  release|rel)
      # Default branch is 'gpdb' as it is our main branch
      if [ -z "${branch}" ]; then
          branch="gpdb"
      fi
      pipeline_type="rel"
      config_file="release.yml"
      hook_res="${proj_name}_commit"
    ;;
  *)
      usage ""
    ;;
esac

yml_path="/tmp/${proj_name}.yml"
my_path=$(realpath "${BASH_SOURCE[0]}")
ytt_base=$(dirname "${my_path}")/pipeline
# pipeline cannot contain '/'
pipeline_name=${pipeline_name/\//"_"}

# Generate pipeline name
if [ -n "${test_suffix}" ]; then
    pipeline_type="${pipeline_type}_test"
fi
pipeline_name="${pipeline_type}.${proj_name}"
if [ -n "${branch}" ]; then
    pipeline_name="${pipeline_name}.${branch}"
fi
if [ -n "${postfix}" ]; then
    pipeline_name="${pipeline_name}.${postfix}"
fi
# pipeline cannot contain '/'
pipeline_name=${pipeline_name/\//"_"}

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
    -v "${proj_name}-branch=${branch}"
set +v

if [ "${pipeline_config}" == "dev" ]; then
    exit 0
fi

concourse_url=$(fly targets | awk "{if (\$1 == \"${target}\") {print \$2}}")
echo ""
echo "================================================================================"
echo "Remeber to set the the webhook URL on GitHub:"
echo "${concourse_url}/api/v1/teams/${concourse_team}/pipelines/${pipeline_name}/resources/${hook_res}/check/webhook?webhook_token=<hook_token>"
echo "You may need to change the base URL if a differnt concourse server is used."
echo "================================================================================"
