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
        sh "bash workflow_inputs.sh"
      }
    }
    stage('Post webhook') {
      steps {
        // Post webhook to trigger self-hosted workflow run
        // echo "Stop"
        def response = sh('curl -L \
            -X POST \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            https://api.github.com/repos/logicalclocks/loadtest/actions/workflows/e2e_small.yaml/dispatches \
            -d @inputs.json', returnStdout: true).trim()
        // export WORKFLOW_RUN_ID=$(echo $response | jq -r '.id')
        def jsonResponse = readJSON text: response
        WORKFLOW_RUN_ID = jsonResponse.id
      }
    }
    stage('Wait for github action workflow to complete') {
      steps {
        script {
          def status = 'in_progress'
          while (status == 'in_progress') {
            sleep 10
            sh 'printenv'
            def response = sh('curl -L -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "X-GitHub-Api-Version: 2022-11-28" "https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}"', returnStdout: true).trim()
            def jsonResponse = readJSON text: response
            status = jsonResponse.status
          }
        }
      }
    }
  }
  post {
    always {
      rm inputs.json

      script {
        def url = sh('curl -L -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}/artifacts | jq -r --arg name "results_${WORKFLOW_RUN_ID}.xml" \'.artifacts[] | select(.name == $name) | .archive_download_url\'', returnStdout: true).trim()
        sh 'curl -L -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "X-GitHub-Api-Version: 2022-11-28" -o results.zip "$url"'
        sh 'unzip results.zip'
      }
      junit 'results.xml'
    }
  }
}
