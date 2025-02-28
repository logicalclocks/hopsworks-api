#!/bin/bash
set -e

echo "Triggering e2e_small workflow from loadtest repository"
curl -L -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    https://api.github.com/repos/logicalclocks/loadtest/actions/workflows/e2e_small.yaml/dispatches \
    -d @inputs.json > response.json

cat response.json

