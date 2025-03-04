#!/bin/bash 
set -e

SHORT_SHA=$1
echo "" > inputs.yaml

if ( [[ ${ghprbSourceBranch} == "FSTORE-" ]] || [[ ${ghprbSourceBranch} == "HWORKS-" ]] ); then
  loadtest_branch=${ghprbSourceBranch}
else
  loadtest_branch="main"
fi

curl -L -G \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -d "state=open" \
  https://api.github.com/repos/logicalclocks/loadtest/pulls > loadtest_prs.json

# echo "${loadtest_prs}" > loadtest_prs.json
cat loadtest_prs.json

# loadtest_branch=$(jq -r --arg api_branch ${ghprbSourceBranch} '.[] | select(.head.ref == $api_branch)' loadtest_prs.json)

if [ -z "${loadtest_branch}" ]; then
  loadtest_branch="main"
fi

cat loadtest_prs.json
echo "${loadtest_branch}"

yq '.ref = "main"' -i inputs.yaml
yq '.inputs.max_parallel = "8"' -i inputs.yaml
hopsworks_domain="10.87.41.190" yq '.inputs.hopsworks_domain = strenv(hopsworks_domain)' -i inputs.yaml
labels="['e2e_small']" yq  '.inputs.labels = strenv(labels)' -i inputs.yaml
hopsworks_api_branch=${ghprbSourceBranch} yq '.inputs.hopsworks_api_branch = strenv(hopsworks_api_branch)' -i inputs.yaml
loadtest_branch=${loadtest_branch} yq '.inputs.loadtest_branch = strenv(loadtest_branch)' -i inputs.yaml
short_sha=$SHORT_SHA yq '.inputs.short_sha = strenv(short_sha)' -i inputs.yaml
# repo_api_author=${ghprbPullAuthorLogin} yq '.inputs.repo_api_author = strenv(repo_api_author)' -i inputs.yaml

yq -o=json inputs.yaml > inputs.json
cat inputs.json