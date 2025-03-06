def WORKFLOW_RUN_ID = "0"
def SHORT_SHA = ""
def REF_LOADTEST_BRANCH = ""
def WORKFLOW_RUN_URL = ""

pipeline("E2E workflows") {
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
        script {
          SHORT_SHA = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
          echo "Short sha: ${SHORT_SHA}"
          sh "bash .github/workflow_inputs.sh ${SHORT_SHA}"
          REF_LOADTEST_BRANCH = sh(script: "cat inputs.json | jq -r '.ref'", returnStdout: true).trim()
          echo "Ref loadtest branch: ${REF_LOADTEST_BRANCH}"
        }
      }
    }
    stage('Post webhook') {
      steps {
        script {
          def dispatch_response = sh(script: """curl -L -X POST -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            -d @inputs.json \
            https://api.github.com/repos/logicalclocks/loadtest/actions/workflows/e2e_small.yaml/dispatches""",
            returnStdout: true
          ).trim()
          echo "Dispatch response: ${dispatch_response}"
          sh "rm inputs.json"
        }
      }
    }
    stage ('Find workflow run id') {
      steps {
        script {
          sleep 5
          TIME_AFTER_WORKFLOW_DISPATCH = sh(script: "date -u +%Y-%m-%dT%H:%M:%SZ", returnStdout: true).trim()
          WORKFLOW_RUN_ID = sh(script: """curl -L -X GET -G -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            -d "event=workflow_dispatch" -d "actor=HopsworksJenkins" -d "branch=${REF_LOADTEST_BRANCH}" \
            https://api.github.com/repos/logicalclocks/loadtest/actions/runs | jq -r '.workflow_runs[0].id'""", returnStdout: true).trim()
          echo "Workflow run id: ${WORKFLOW_RUN_ID}"
        }
      }
    }
    stage('Wait for github action workflow to complete') {
      steps {
        script {
          def status = "in_progress"
          while (status == "in_progress" || status == "queued") {
            sleep 60
            status = sh(script: """curl -L -X GET -H "Accept: application/vnd.github+json" \
              -H "Authorization: Bearer ${GITHUB_TOKEN}" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID} | jq -r '.status' """, returnStdout: true).trim()
            echo "Status: ${status}"
          }
        }
      }
    }
    stage('Download artifacts') {
      steps {
        script {
          def REPORT_URL = sh(
            script: """curl -L -H "Accept: application/vnd.github+json" \
              -H "Authorization: Bearer ${GITHUB_TOKEN}" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}/artifacts \
              | jq -r '.artifacts[] | select(.name == "results_${WORKFLOW_RUN_ID}.xml") | .archive_download_url' """,
            returnStdout: true
          ).trim()
          echo "Report url: ${REPORT_URL}"
          sh(
            script: """curl -L -H \"Accept: application/vnd.github+json\" \
            -H \"Authorization: Bearer ${GITHUB_TOKEN}\" \
            -H \"X-GitHub-Api-Version: 2022-11-28\" \
            -o results.zip "${REPORT_URL}" """
          )
          sh """if [ -f results.xml ]; then rm results.xml; fi && unzip results.zip && rm results.zip"""
        }
      }
    }
  }
  post {
    always {
      junit 'results.xml'
    }
  }
}