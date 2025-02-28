pipeline {
  agent {
    label "local"
  }
  environment {
    GITHUB_TOKEN = credentials('990f5312-cd08-48ec-baf8-3b27ff551204')
  }
  stages {
    stage('Clone repository') {
      steps {
        checkout scm
      }
    }
    stage('Input parameters') {
      steps {
        sh "bash .github/workflow_inputs.sh"
      }
    }
    stage('Post webhook') {
      steps {
        sh "bash .github/trigger_workflow.sh"
      }
    }
    stage('Wait for github action workflow to complete') {
      steps {
        sh "bash .github/wait_for_workflow.sh"
        }
      }
    }
  }
//   post {
    // always {
        // sh 'rm inputs.json && rm response.json && rm workflow_response.json'
//         sh """ curl -L -H "Accept: application/vnd.github+json" \
//         -H "Authorization: Bearer ${GITHUB_TOKEN}" \
//         -H "X-GitHub-Api-Version: 2022-11-28" \
//         "https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}/artifacts" > artifacts.json"""
//         sh 'url=$(cat artifacts.json | jq -r ".artifacts[0].archive_download_url") && export REPORT_URL=$url'
//         sh 'curl -L -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "X-GitHub-Api-Version: 2022-11-28" -o results.zip "${REPORT_URL}"'
//         sh 'unzip results.zip'
    //   }
//       junit 'results.xml'
    // }
}