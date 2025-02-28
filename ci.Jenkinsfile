def WORKFLOW_RUN_ID = "0"
def SHORT_SHA = ""
def HEAD_SHA = ""
def REF_LOADTEST_BRANCH = ""
def TIME_BEFORE_WORKFLOW_DISPATCH = ""

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
        script {
          // Triggering workflow dispatch event does not return an identifier which can be used, 
          // therefore the short sha will be used to identify the workflow run 
          TIME_BEFORE_WORKFLOW_DISPATCH = sh(script: "date -u +%Y-%m-%dT%H:%M:%SZ", returnStdout: true).trim()
          SHORT_SHA = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
          HEAD_SHA = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
          echo "Short sha: ${SHORT_SHA}"
          echo "Head sha: ${HEAD_SHA}"
          sh "bash .github/workflow_inputs.sh ${SHORT_SHA}"
          REF_LOADTEST_BRANCH = sh(script: "cat inputs.json | jq -r '.ref'", returnStdout: true).trim()
          echo "Ref loadtest branch: ${REF_LOADTEST_BRANCH}"
        }
      }
    }
    stage('Post webhook') {
      steps {
        script {
          
          sh(script: """curl -L -X POST -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            -d @inputs.json \
            https://api.github.com/repos/logicalclocks/loadtest/actions/workflows/e2e_small.yaml/dispatches""")
        }
      }
    }
    stage ('Find workflow run id') {
      steps {
        script {
          def runs = sh(script: """curl -L -X GET -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            https://api.github.com/repos/logicalclocks/loadtest/actions/runs?event=workflow_dispatch&actor=HopsworksJenkins&branch=${REF_LOADTEST_BRANCH}&created:>${TIME_BEFORE_WORKFLOW_DISPATCH}""", returnStdout: true).trim()
          echo "Runs: ${runs}"
          WORKFLOW_RUN_ID = sh(script: """echo ${runs} | jq -r --arg short_sha "${SHORT_SHA}" '.workflow_runs[] | select(.inputs.short_sha == $short_sha) | .id'""", returnStdout: true).trim()
          echo "Workflow run id: ${WORKFLOW_RUN_ID}"
        }
      }
    }
    stage('Wait for github action workflow to complete') {
      steps {
        script {
          def status = "in_progress"
          while (status == "in_progress") {
            sleep 10
            workflow_payload = sh(script: """curl -L -X GET -H "Accept: application/vnd.github+json" \
              -H "Authorization: Bearer ${GITHUB_TOKEN}" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              https://api.github.com/repos/logicalclocks/loadtest/actions/runs/${WORKFLOW_RUN_ID}""", returnStdout: true).trim()
            echo "Workflow payload: ${workflow_payload}"
            status = sh(script: "echo ${workflow_payload} | jq -r '.status'", returnStdout: true).trim()
            echo "Status: ${status}"
          }
        }
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