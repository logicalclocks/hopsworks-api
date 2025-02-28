#!/bin/bash
set -e

WORKFLOW_RUN_ID=$(cat workflow_response.json | jq -r ".id")
echo "Workflow Run ID: ${WORKFLOW_RUN_ID}"

curl -L -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}/artifacts" > artifacts.json
    
REPORT_URL=$(cat artifacts.json | jq -r ".artifacts[0].archive_download_url")
curl -L -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -o results.zip "${REPORT_URL}"

unzip results.zip