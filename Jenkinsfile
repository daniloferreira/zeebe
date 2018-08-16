// vim: set filetype=groovy:

def mavenSettingsConfig = 'camunda-maven-settings'

def joinJmhResults = '''\
#!/bin/bash -x
cat **/*/jmh-result.json | jq -s add > target/jmh-result.json
'''

def goTests() {
    return '''\
#!/bin/bash -eux
echo "== Go build environment =="
go version
echo "GOPATH=${GOPATH}"

PROJECT_ROOT="${GOPATH}/src/github.com/zeebe-io"
mkdir -p ${PROJECT_ROOT}

PROJECT_DIR="${PROJECT_ROOT}/zeebe"
ln -fvs ${WORKSPACE} ${PROJECT_DIR}

cd ${GOPATH}/src/github.com/zeebe-io/zeebe/clients/go
make install-deps test
'''
}

pipeline {
    agent {
        kubernetes {
            cloud 'zeebe-ci'
            label "zeebe-ci-build_${env.JOB_BASE_NAME.replaceAll("%2F", "-").take(20)}-${env.BUILD_ID}"
            defaultContainer 'jnlp'
            yamlFile '.ci/podSpecs/builderAgent.yml'
        }
    }

    options {
        buildDiscarder(logRotator(daysToKeepStr:'14', numToKeepStr:'10'))
        timestamps()
        timeout(time: 45, unit: 'MINUTES')
    }

    stages {
        stage('Install') {
            steps {
                container('maven') {
                    // MaxRAMFraction = LIMITS_CPU because there are only maven build threads
                    sh '''\
export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -XX:MaxRAMFraction=$((LIMITS_CPU))"
mvn -B -T 1C clean com.mycila:license-maven-plugin:check com.coveo:fmt-maven-plugin:check install -DskipTests
'''

                    stash name: "zeebe-dist", includes: "dist/target/zeebe-broker/**/*"
                }
            }
        }

        stage('Verify') {
            failFast true
            parallel {
                stage('1 - Java Tests') {
                    steps {
                        container('maven') {
                            // MaxRAMFraction = LIMITS_CPU+1 because there are LIMITS_CPU surefire threads + one maven thread
                            sh '''\
export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -XX:MaxRAMFraction=$((LIMITS_CPU+1))"
mvn -B -T 1C verify -P skip-unstable-ci,retry-tests,parallel-tests
'''
                        }
                    }
                    post {
                        failure {
                            archiveArtifacts artifacts: '**/target/*-reports/**/*-output.txt,**/**/*.dumpstream', allowEmptyArchive: true
                        }
                    }
                }
            }
        }
    }

    post {
        changed {
            sendBuildStatusNotificationToDevelopers(currentBuild.result)
        }
    }
}

void sendBuildStatusNotificationToDevelopers(String buildStatus = 'SUCCESS') {
    def buildResult = buildStatus ?: 'SUCCESS'
    def subject = "${buildResult}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'"
    def details = "${buildResult}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' see console output at ${env.BUILD_URL}'"

    emailext (
        subject: subject,
        body: details,
        recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
    )
}
