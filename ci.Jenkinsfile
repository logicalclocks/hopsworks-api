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