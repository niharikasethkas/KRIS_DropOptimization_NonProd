#!/usr/bin/env groovy
pipeline {
  agent none

  options {
    disableConcurrentBuilds()
  }

  environment {
    AWS_DEFAULT_REGION = 'ap-southeast-2'
    PROJECT_ID = 'analytics'
    SLACK_NOTIFY_CHANNEL = 'G014L87QFFA' // # ml-platform-notify
    CFN_ECR_REPO_NAME = 'analytics/kris_do_nonprod'
    PROJECT_NAME = 'kris_do_nonprod'
    ECR_ENDPOINT = 'https://847029211010.dkr.ecr.ap-southeast-2.amazonaws.com'
    CFN_ENVIRONMENT = 'datasvcsprod'
    CFN_ARTEFACT_ENVIRONMENT = 'artefact'
    CFN_HOSTED_ZONE = 'int.ap-southeast-2.datasvcsprod.a-sharedinfra.net'
    ACCOUNT_ID = '978148700146'
    ECR_FLAG = 'true'
    SKIP_FLAG = 'false'
  }

stages {
    stage ('Parallel Infrastructure Linting') {
      agent {
        label 'ec2-amazonlinux2'
      }
      steps {
        script {
          node {
            // To avoid issues with the Jenkins ECR plugin
            sh 'echo "$SLACK_NOTIFY_CHANNEL"'
            sh 'rm -f "${HOME}/.dockercfg" || true'
            checkout scm
            def builds = [:]
              builds['shellcheck'] = {
                ->
                docker.image("${ECR_HOST}/sharedtools/shellcheck:latest").inside {
                  sh './scripts/shellcheck.sh'
                }
              }
              builds['cloudformation'] = {
                ->
                docker.image("${ECR_HOST}/sharedtools/cfn_manage:latest").inside {
                  sh './scripts/cfn_validate.sh'
                }
              }
              withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
                credentialsId: "${env.PROJECT_ID}-aws-${env.CFN_ENVIRONMENT}"]]) {
                parallel builds
              }
          }
        }
      }
    }

stage('Deploy ECR') {
       agent {
         label 'ec2-amazonlinux2'
       }
       when {
         expression { env.ECR_FLAG == 'true' }
       }
       steps {
         script {
           withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
             credentialsId: "$env.PROJECT_ID-aws-${env.CFN_ARTEFACT_ENVIRONMENT}"]]) {
             docker.image("${ECR_HOST}/sharedtools/cfn_manage:latest").inside {
               sh './scripts/deploy_ecr.sh'
             }
           }
         }
       }
     }
    stage('Building Docker Image Airflow: kris_do_nonprod') {
      agent {
        label 'ec2-amazonlinux2'
      }
      when {
        expression { env.SKIP_FLAG == 'false' }
      }
      steps {
        script{
          checkout scm
          withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
          credentialsId: "${env.PROJECT_ID}-aws-${env.CFN_ENVIRONMENT}"]]) {
            env['sf_db_password'] =  sh(script: '''
                  echo $(aws secretsmanager get-secret-value --secret-id SNOWFLAKE-PASSWORD --region ap-southeast-2 | jq -r .SecretString)
                  ''', returnStdout: true).trim()
          }
        }
        script {
          node {
            checkout scm
            def dockerBuild = docker.build("${env.CFN_ECR_REPO_NAME}:airflow_prod","--no-cache --build-arg http_proxy=${env.http_proxy} --build-arg SF_DB_PASSWORD=${env.sf_db_password} --build-arg https_proxy=${env.https_proxy} --build-arg no_proxy=${env.no_proxy} -f ./Dockerfile_airflow .")
              withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
              credentialsId: "${env.PROJECT_ID}-aws-${env.CFN_ARTEFACT_ENVIRONMENT}"]]) {
              docker.withRegistry("${env.ECR_ENDPOINT}") {
                dockerBuild.push()
              }
            }
          }
        }
      }
    }
    stage('Deploy Airflow Dags') {
      agent {
        label 'ec2-amazonlinux2'
      }
      steps {
        script {
          node {
            checkout scm
            sshagent(credentials: ['data-services-key']) {
              withCredentials([usernamePassword(credentialsId: 'github-app-sharedsvc',usernameVariable: 'GITHUB_APP', 
              passwordVariable: 'GITHUB_ACCESS_TOKEN')]) {
                sh 'rm -rf kris_do_nonprod && git clone -b prod https://${GITHUB_APP}:${GITHUB_ACCESS_TOKEN}@github.com/KmartAU/kris_do_nonprod.git'
                sh 'pwd && cd kris_do_nonprod && ls -l'
              }
            }
            withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', 
            credentialsId: "${env.PROJECT_ID}-aws-${env.CFN_ENVIRONMENT}"]]) {
               docker.image("${ECR_HOST}/sharedtools/cfn_manage:latest").inside(' -u 0') {
                 sh './scripts/deploy_airflow_dags.sh'
               }
             }
      }
    }
  }
 }     
}
}
