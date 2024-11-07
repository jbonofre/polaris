/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

plugins {
  alias(libs.plugins.quarkus)
  id("polaris-server")
}

dependencies {
  implementation(project(":polaris-core"))
  // TODO @RuntimeCandidate is in polaris-service-quarkus, maybe move a polaris-spi module ?
  implementation(project(":polaris-service-quarkus"))
  implementation(libs.eclipselink)
  implementation(platform(libs.quarkus.bom))

  implementation(libs.slf4j.api)
  implementation(libs.guava)

  // TODO used for Discoverable, it will be removed with Dropwizard
  implementation(platform(libs.dropwizard.bom))
  implementation("io.dropwizard:dropwizard-core")

  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly("io.quarkus:quarkus-arc")
  compileOnly("org.eclipse.microprofile.config:microprofile-config-api")

  testImplementation(libs.h2)
  testImplementation(testFixtures(project(":polaris-core")))

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
