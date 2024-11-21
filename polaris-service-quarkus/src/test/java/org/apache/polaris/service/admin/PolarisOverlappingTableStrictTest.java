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
package org.apache.polaris.service.admin;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(PolarisOverlappingTableStrictTest.Profile.class)
public class PolarisOverlappingTableStrictTest extends PolarisOverlappingTableTestBase {

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  @DisplayName("STRICT - Test restrictions on table locations")
  void testTableLocationRestrictionsWithNamespace(CatalogType catalogType, Response.Status status) {
    super.testTableLocationRestrictions(catalogType, status);
  }

  private Stream<Arguments> getTestConfigs() {
    return getTestConfigs(Response.Status.FORBIDDEN, Response.Status.FORBIDDEN, Response.Status.OK);
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.config.feature-configurations.ALLOW_UNSTRUCTURED_TABLE_LOCATION", "false",
          "polaris.config.feature-configurations.ALLOW_TABLE_LOCATION_OVERLAP", "false");
    }
  }
}
