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

import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(PolarisOverlappingCatalogTest.Profile.class)
public class PolarisOverlappingCatalogTest {

  @Inject PolarisIntegrationTestHelper testHelper;

  @BeforeAll
  public void setUp(TestInfo testInfo) {
    testHelper.setUp(testInfo);
  }

  @AfterAll
  public void tearDown() {
    testHelper.tearDown();
  }

  private Response createCatalog(String prefix, String defaultBaseLocation, boolean isExternal) {
    return createCatalog(prefix, defaultBaseLocation, isExternal, new ArrayList<String>());
  }

  private Invocation.Builder request() {
    return testHelper
        .client
        .target(
            String.format("http://localhost:%d/api/management/v1/catalogs", testHelper.localPort))
        .request("application/json")
        .header("Authorization", "Bearer " + testHelper.adminToken)
        .header(REALM_PROPERTY_KEY, testHelper.realm);
  }

  private Response createCatalog(
      String prefix,
      String defaultBaseLocation,
      boolean isExternal,
      List<String> allowedLocations) {
    String uuid = UUID.randomUUID().toString();
    StorageConfigInfo config =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(
                allowedLocations.stream()
                    .map(
                        l -> {
                          return String.format("s3://bucket/%s/%s", prefix, l);
                        })
                    .toList())
            .build();
    Catalog catalog =
        new Catalog(
            isExternal ? Catalog.TypeEnum.EXTERNAL : Catalog.TypeEnum.INTERNAL,
            String.format("overlap_catalog_%s", uuid),
            new CatalogProperties(String.format("s3://bucket/%s/%s", prefix, defaultBaseLocation)),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            config);
    try (Response response = request().post(Entity.json(new CreateCatalogRequest(catalog)))) {
      return response;
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  public void testBasicOverlappingCatalogs(boolean initiallyExternal, boolean laterExternal) {
    String prefix = UUID.randomUUID().toString();

    assertThat(createCatalog(prefix, "root", initiallyExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // OK, non-overlapping
    assertThat(createCatalog(prefix, "boot", laterExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // OK, non-overlapping due to no `/`
    assertThat(createCatalog(prefix, "roo", laterExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // Also OK due to no `/`
    assertThat(createCatalog(prefix, "root.child", laterExternal))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // inside `root`
    assertThat(createCatalog(prefix, "root/child", laterExternal))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // `root` is inside this
    assertThat(createCatalog(prefix, "", laterExternal))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  public void testAllowedLocationOverlappingCatalogs(
      boolean initiallyExternal, boolean laterExternal) {
    String prefix = UUID.randomUUID().toString();

    assertThat(createCatalog(prefix, "animals", initiallyExternal, Arrays.asList("dogs", "cats")))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // OK, non-overlapping
    assertThat(createCatalog(prefix, "danimals", laterExternal, Arrays.asList("dan", "daniel")))
        .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

    // This DBL overlaps with initial AL
    assertThat(createCatalog(prefix, "dogs", initiallyExternal, Arrays.asList("huskies", "labs")))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // This AL overlaps with initial DBL
    assertThat(
            createCatalog(
                prefix, "kingdoms", initiallyExternal, Arrays.asList("plants", "animals")))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    // This AL overlaps with an initial AL
    assertThat(createCatalog(prefix, "plays", initiallyExternal, Arrays.asList("rent", "cats")))
        .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.config.feature-configurations.ALLOW_OVERLAPPING_CATALOG_URLS", "false");
    }
  }
}
