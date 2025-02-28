def WORKFLOW_RUN_ID = "0"

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
        script {
          def response = sh(script: """curl -L -X POST -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            -d input.json \
            https://api.github.com/repos/logicalclocks/loadtest/actions/workflows/e2e_small.yaml/dispatches""", returnStdout: true).trim()
          echo "Response: ${response}"
        }
        // sh "bash .github/trigger_workflow.sh"
      }
    }
    stage('Wait for github action workflow to complete') {
      steps {
        sh "bash .github/wait_for_workflow_run.sh"
      }
    }
    stage('Download artifacts') {
      steps {
        sh "bash .github/download_artifacts.sh"
      }
    }
  }
  post {
    always {
      sh "bash .github/cleanup.sh"
      junit 'results.xml'
    }
  }
}