#!groovy
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Variables expected from Jenkins :
// "${PagerDuty}", {"true","false"}
// "${PagerDutyEndpointURL}", {"https://events.pagerduty.com/...."}

def sendPagerDutyEvent(msg) {

      // PagerDuty settings
      def pdEndpoint = "${PagerDutyEndpointURL}"
      def pdRequest = [
          "event_type": "trigger",
          "incident_key": "WHISK/CICD/Images2Dockerhub",
          "description": msg
      ]

      println("Sending PD event to ${pdEndpoint}")

      println("pdRequest=" + pdRequest)

      // get PD service key
      withCredentials([[$class: 'StringBinding', credentialsId: 'PD_SERVICE_KEY_CICD', variable: 'pdServiceKey']]) {
          pdRequest["service_key"] = env.pdServiceKey
      }

      // send request to PD api and get response
/*
      def response = httpRequest consoleLogResponseBody: true,
                                 contentType: 'APPLICATION_JSON',
                                 httpMode: 'POST',
                                 requestBody: groovy.json.JsonOutput.toJson(pdRequest),
                                 url: pdEndpoint

      if (response.status != 200) {
        println("Error: Request to send PD alert failed rc=" + response.status + "text=" + response.content)
      }
*/
} // end sendPagerDutyEvent


timeout(time: 30, unit: 'MINUTES') {
  node('cf_slave') {
    sh "env"
    sh "docker version"
    sh "docker info"

    checkout scm

    try {

      stage("Build and Deploy to DockerHub") {

          println("PagerDuty=${PagerDuty}")

          sendPagerDutyEvent("OpenWhisk-DockerHub started - See Build ${env.BUILD_NUMBER} for details - ${env.BUILD_URL}")

          withCredentials([usernamePassword(credentialsId: 'openwhisk_dockerhub', passwordVariable: 'DOCKER_PASSWORD', usernameVariable: 'DOCKER_USER')]) {
              sh 'docker login -u ${DOCKER_USER} -p ${DOCKER_PASSWORD}'
          }
          def PUSH_CMD = "./gradlew :core:controller:distDocker :core:invoker:distDocker :core:standalone:distDocker :core:monitoring:user-events:distDocker :tools:ow-utils:distDocker :core:cosmos:cache-invalidator:distDocker -PdockerRegistry=docker.io -PdockerImagePrefix=ibmfunctions"
          def gitCommit = sh(returnStdout: true, script: 'git rev-parse HEAD').trim()
          def shortCommit = gitCommit.take(7)
          sh "./gradlew clean"
          sh "${PUSH_CMD} -PdockerImageTag=nightly"
          sh "${PUSH_CMD} -PdockerImageTag=${shortCommit}"
      }

      stage("Clean") {
        sh "docker images"
        sh 'docker rmi -f $(docker images -f "reference=openwhisk/*" -q) || true'
        sh "docker images"
      }

      stage("Notify") {
        println("Done.")
      }

    } catch (e) {

      if ("${PagerDuty}" != 'false') {
        println("Error: Problem during build, prepare and send a PagerDuty alert.")
        sendPagerDutyEvent("OpenWhisk-DockerHub is unstable / failed - See Build ${env.BUILD_NUMBER} for details - ${env.BUILD_URL}")
      } else {
        println("PagerDuty alert skipped")
      }

      throw e // fails the build and prints stack trace

    } /* end catch */

  } // node
} // timeout
