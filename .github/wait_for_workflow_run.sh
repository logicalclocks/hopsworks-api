#!/bin/bash

status='in_progress'
WORKFLOW_RUN_ID=$(cat response.json | jq -r ".id")
echo "Workflow Run ID: ${WORKFLOW_RUN_ID}"
while [ "${status}" == 'in_progress' ]; do
  sleep 10
  printenv
  curl -L -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}" > workflow_response.json
  cat workflow_response.json
  status=$(jq -r '.status' workflow_response.json)
  echo "Status: ${status}"
done


echo "Workflow Run completed:"
cat workflow_response.json
