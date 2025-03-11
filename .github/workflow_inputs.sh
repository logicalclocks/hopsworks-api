#!/bin/bash 
set -e

SHORT_SHA=$1
echo "" > inputs.yaml


if [[ ${ghprbPullTitle} =~ (FSTORE-[0-9]+) || ${ghprbPullTitle} =~ (HWORKS-[0-9]+) ]]; then
  captured_string=${BASH_REMATCH[1]}
  echo "Found JIRA ticket: ${captured_string}, checking for corresponding pr in loadtest repo"
  loadtest_prs=$(curl -L -G \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -d "state=open" \
    https://api.github.com/repos/logicalclocks/loadtest/pulls)

  loadtest_branch=$(echo "${loadtest_prs}" | jq -r --arg captured_string ${captured_string} '.[] | select(.title | contains($captured_string)) | .head.ref')
  minikube_ip=$(echo "${loadtest_prs}" | jq -r --arg captured_string ${captured_string} '.[] | select(.title | contains($captured_string)) | .labels[] | select(.name | contains("10.87.")) | .name')
  labels=$(echo "${loadtest_prs}" | jq -r --arg captured_string ${captured_string} '.[] | select(.title | contains($captured_string)) | .labels[] | select(.name | contains("e2e")) | .name' | paste -sd ",")
fi

if [ -z "${loadtest_branch}" ]; then
    echo "No corresponding pr found in loadtest repo, using main branch"
    loadtest_branch="main"
  else
    echo "Found loadtest branch: ${loadtest_branch}"
  fi

  if [ -z "${minikube_ip}" ]; then
    echo "No minikube ip found in labels, using default staging cluster" 
    minikube_ip="stagingmain.devnet.hops.works" # Make it domain name instead of ip
  else
    echo "Found minikube ip in loadtest PR labels: ${minikube_ip}"
  fi

  if [ -z "${labels}" ]; then
    echo "No labels found, using default e2e_small" 
    labels="e2e_small"
  else
    echo "Found labels: ${labels}"
  fi

# .ref is the name of the branch where the workflow dispatch will be sent.
yq '.ref = "main"' -i inputs.yaml

yq '.inputs.max_parallel = "5"' -i inputs.yaml
hopsworks_domain=$minikube_ip yq '.inputs.hopsworks_domain = strenv(hopsworks_domain)' -i inputs.yaml
labels=$labels yq  '.inputs.labels = strenv(labels)' -i inputs.yaml
hopsworks_api_branch=${ghprbSourceBranch} yq '.inputs.hopsworks_api_branch = strenv(hopsworks_api_branch)' -i inputs.yaml
loadtest_branch=${loadtest_branch} yq '.inputs.loadtest_head_ref = strenv(loadtest_branch)' -i inputs.yaml
short_sha=$SHORT_SHA yq '.inputs.short_sha = strenv(short_sha)' -i inputs.yaml
user_repo_api=${ghprbPullAuthorLogin} yq '.inputs.user_repo_api = strenv(user_repo_api)' -i inputs.yaml

yq -o=json inputs.yaml > inputs.json
cat inputs.json