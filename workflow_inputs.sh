#!/bin/bash 
set -e

echo "" > inputs.yaml

if ( [[ ${ghprbSourceBranch} == "FSTORE-" ]] || [[ ${ghprbSourceBranch} == "HWORKS-" ]] ); then
  loadtest_branch=${ghprbSourceBranch}
else
  loadtest_branch="main"
fi


loadtest_prs=$(curl -L \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/logicalclocks/loadtest/pulls) > loadtest_prs.json

loadtest_branch=$(jq -r --arg api_branch ${ghprbSourceBranch} '.[] | select(.head.ref == $api_branch)' loadtest_prs.json) || 'main'

cat loadtest_prs.json
echo "${loadtest_branch}"

yq '.ref = "main"' -i inputs.yaml
yq '.inputs.max_parallel = "8"' -i inputs.yaml
hopsworks_domain="10.87.41.158" yq '.inputs.hopsworks_domain = strenv(hopsworks_domain)' -i inputs.yaml
labels="['e2e_small']" yq  '.inputs.labels = strenv(labels)' -i inputs.yaml
hopsworks_api_branch=${ghprbSourceBranch} yq '.inputs.hopsworks_api_branch = strenv(hopsworks_api_branch)' -i inputs.yaml
loadtest_branch=${loadtest_branch} yq '.inputs.loadtest_branch = strenv(loadtest_branch)' -i inputs.yaml
short_sha=$(git rev-parse --short HEAD) yq '.inputs.short_sha = strenv(short_sha)' -i inputs.yaml

yq -o=json inputs.yaml > inputs.json
cat inputs.json