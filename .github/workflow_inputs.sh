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
  minikube_ip_labels=$(echo "${loadtest_prs}" | jq -r --arg captured_string ${captured_string} '.[] | select(.title | contains($captured_string)) | .labels[] | select(.name | contains("10.87.")) | .name') 
  
  echo "Found labels: ${input_labels}"

  if [ -z "${loadtest_branch}" ]; then
    echo "No corresponding pr found in loadtest repo, using main branch"
    loadtest_branch="main"
  else
    echo "Found loadtest branch: ${loadtest_branch}"
  fi

  if [ -z "${minikube_ip_labels}" ]; then
    echo "No minikube ip labels found, using default staging cluster" 
    minikube_ip_labels="10.87.41.126" # Make it domain name instead of ip
  else
    echo "Found minikube ip labels: ${minikube_ip_labels}"
  fi
else
  loadtest_branch="main"
fi


# loadtest_prs=$(curl -L -G \
#   -H "Accept: application/vnd.github+json" \
#   -H "Authorization: Bearer ${GITHUB_TOKEN}" \
#   -H "X-GitHub-Api-Version: 2022-11-28" \
#   -d "state=open" \
#   https://api.github.com/repos/logicalclocks/loadtest/pulls)

# echo ${loadtest_prs}

# loadtest_pr=$(echo ${loadtest_prs} | jq -r --arg api_branch ${ghprbSourceBranch} '.[] | select(.head.ref == $api_branch)')
# echo ${loadtest_pr}
#   || jq -r --arg api_branch ${ghprbSourceBranch} '.[] | select(.head.ref == $api_branch)')

# if [ -z "${loadtest_branch}" ]; then
#   loadtest_branch="main"
# fi

# cat loadtest_prs.json
echo "${loadtest_branch}"

# .ref is the name of the branch where the workflow dispatch will be sent.
yq '.ref = "jenkins-ci"' -i inputs.yaml

yq '.inputs.max_parallel = "8"' -i inputs.yaml
hopsworks_domain="10.87.40.126" yq '.inputs.hopsworks_domain = strenv(hopsworks_domain)' -i inputs.yaml
labels='e2e_small' yq  '.inputs.labels = strenv(labels)' -i inputs.yaml
hopsworks_api_branch=${ghprbSourceBranch} yq '.inputs.hopsworks_api_branch = strenv(hopsworks_api_branch)' -i inputs.yaml
loadtest_branch=${loadtest_branch} yq '.inputs.loadtest_branch = strenv(loadtest_branch)' -i inputs.yaml
short_sha=$SHORT_SHA yq '.inputs.short_sha = strenv(short_sha)' -i inputs.yaml
user_repo_api=${ghprbPullAuthorLogin} yq '.inputs.user_repo_api = strenv(user_repo_api)' -i inputs.yaml

yq -o=json inputs.yaml > inputs.json
cat inputs.json