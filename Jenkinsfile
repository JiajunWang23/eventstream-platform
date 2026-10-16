// Declarative pipeline: build -> test -> package image -> push -> deploy.
// Required Jenkins credentials:
//   docker-registry   (username/password)  - registry login
//   deploy-ssh        (ssh private key)    - deploy host access
pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 30, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '20'))
        disableConcurrentBuilds()
    }

    environment {
        REGISTRY     = 'ghcr.io/jiajunwang23'
        IMAGE        = "${REGISTRY}/eventstream-platform"
        IMAGE_TAG    = "${env.BRANCH_NAME == 'main' ? 'latest' : env.BRANCH_NAME}-${env.GIT_COMMIT?.take(7)}"
        DEPLOY_HOST  = 'deploy@eventstream.internal'
        DEPLOY_DIR   = '/opt/eventstream-platform'
    }

    tools {
        jdk 'temurin-21'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                sh 'git rev-parse --short HEAD > .git-sha'
            }
        }

        stage('Build & Unit Test') {
            steps {
                sh './gradlew clean build --no-daemon --stacktrace'
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'build/test-results/test/*.xml'
                    publishHTML target: [
                        allowMissing: true, alwaysLinkToLastBuild: true, keepAll: true,
                        reportDir: 'build/reports/jacoco/test/html', reportFiles: 'index.html',
                        reportName: 'JaCoCo Coverage'
                    ]
                }
            }
        }

        stage('Integration Tests') {
            when { anyOf { branch 'main'; changeRequest() } }
            steps {
                // Testcontainers spins up Kafka + MongoDB on the agent's Docker daemon.
                sh './gradlew integrationTest --no-daemon'
            }
        }

        stage('Build Image') {
            steps {
                sh "docker build -t ${IMAGE}:${IMAGE_TAG} ."
            }
        }

        stage('Push Image') {
            when { anyOf { branch 'main'; tag 'v*' } }
            steps {
                withCredentials([usernamePassword(credentialsId: 'docker-registry',
                                                  usernameVariable: 'REG_USER',
                                                  passwordVariable: 'REG_PASS')]) {
                    sh 'echo "$REG_PASS" | docker login ghcr.io -u "$REG_USER" --password-stdin'
                    sh "docker push ${IMAGE}:${IMAGE_TAG}"
                    sh "docker tag ${IMAGE}:${IMAGE_TAG} ${IMAGE}:latest && docker push ${IMAGE}:latest"
                }
            }
        }

        stage('Deploy') {
            when { branch 'main' }
            steps {
                sshagent(credentials: ['deploy-ssh']) {
                    sh """
                        scp -o StrictHostKeyChecking=no docker-compose.yml ${DEPLOY_HOST}:${DEPLOY_DIR}/docker-compose.yml
                        ssh -o StrictHostKeyChecking=no ${DEPLOY_HOST} '
                            cd ${DEPLOY_DIR} &&
                            export DOCKER_IMAGE=${IMAGE}:${IMAGE_TAG} &&
                            docker compose pull app &&
                            docker compose up -d --no-build app &&
                            docker image prune -f
                        '
                    """
                }
            }
        }

        stage('Smoke Test') {
            when { branch 'main' }
            steps {
                sh 'scripts/smoke-test.sh http://eventstream.internal:8080'
            }
        }
    }

    post {
        success { echo "Deployed ${IMAGE}:${IMAGE_TAG}" }
        failure { echo "Pipeline failed on ${env.BRANCH_NAME} @ ${env.GIT_COMMIT}" }
        cleanup { cleanWs() }
    }
}
