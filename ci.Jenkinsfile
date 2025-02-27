pipeline {
  agent {
    label "local"
  }
  stage('Clone repository') {
    checkout scm
  }
  stage('Input parameters') {
    sh "bash workflow_inputs.sh"
  }
  stage('Dispatch self-hosted workflow run') {
    environment {
        GITHUB_TOKEN = credentials('990f5312-cd08-48ec-baf8-3b27ff551204')
    }
    steps {
      // Post webhook to trigger self-hosted workflow run
      sh 'cat inputs.json && curl -L \
          -X POST \
          -H "Accept: application/vnd.github+json" \
          -H "Authorization: Bearer ${GITHUB_TOKEN}" \
          -H "X-GitHub-Api-Version: 2022-11-28" \
          https://api.github.com/repos/logicalclocks/loadtest/actions/workflows/e2e_small/dispatches \
          -d @inputs.json'
    }
  }
}
