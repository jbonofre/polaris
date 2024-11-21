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
package org.apache.polaris.service.catalog;

import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.service.auth.BasePolarisAuthenticator;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PolarisRestCatalogViewIntegrationTest extends ViewCatalogTests<RESTCatalog> {
  public static final String TEST_ROLE_ARN =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN"))
          .orElse("arn:aws:iam::123456789012:role/my-role");
  public static final String S3_BUCKET_BASE =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_S3_PATH"))
          .orElse("file:///tmp/buckets/my-bucket");

  private RESTCatalog restCatalog;

  @Inject PolarisIntegrationTestHelper testHelper;

  @BeforeAll
  public void setUp(TestInfo testInfo) {
    testHelper.setUp(testInfo);
  }

  @BeforeEach
  public void setUpTempDir(@TempDir Path tempDir) throws Exception {
    // see https://github.com/quarkusio/quarkus/issues/13261
    Field field = ViewCatalogTests.class.getDeclaredField("tempDir");
    field.setAccessible(true);
    field.set(this, tempDir);
  }

  @BeforeEach
  void before(TestInfo testInfo) {
    testInfo
        .getTestMethod()
        .ifPresent(
            method -> {
              String catalogName = method.getName();
              try (Response response =
                  testHelper
                      .client
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s",
                              testHelper.localPort, catalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + testHelper.adminToken)
                      .header(REALM_PROPERTY_KEY, testHelper.realm)
                      .get()) {
                if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                  // Already exists! Must be in a parameterized test.
                  // Quick hack to get a unique catalogName.
                  // TODO: Have a while-loop instead with consecutive incrementing suffixes.
                  catalogName = catalogName + System.currentTimeMillis();
                }
              }

              AwsStorageConfigInfo awsConfigModel =
                  AwsStorageConfigInfo.builder()
                      .setRoleArn(TEST_ROLE_ARN)
                      .setExternalId("externalId")
                      .setUserArn("userArn")
                      .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                      .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
                      .build();
              org.apache.polaris.core.admin.model.CatalogProperties props =
                  org.apache.polaris.core.admin.model.CatalogProperties.builder(
                          S3_BUCKET_BASE + "/" + System.getenv("USER") + "/path/to/data")
                      .addProperty(
                          CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY,
                          "file:")
                      .addProperty(
                          PolarisConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
                          "true")
                      .addProperty(
                          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                          "true")
                      .build();
              Catalog catalog =
                  PolarisCatalog.builder()
                      .setType(Catalog.TypeEnum.INTERNAL)
                      .setName(catalogName)
                      .setProperties(props)
                      .setStorageConfigInfo(
                          S3_BUCKET_BASE.startsWith("file:")
                              ? new FileStorageConfigInfo(
                                  StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://"))
                              : awsConfigModel)
                      .build();
              try (Response response =
                  testHelper
                      .client
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs",
                              testHelper.localPort))
                      .request("application/json")
                      .header("Authorization", "Bearer " + testHelper.adminToken)
                      .header(REALM_PROPERTY_KEY, testHelper.realm)
                      .post(Entity.json(catalog))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }
              CatalogRole newRole = new CatalogRole("admin");
              try (Response response =
                  testHelper
                      .client
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles",
                              testHelper.localPort, catalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + testHelper.adminToken)
                      .header(REALM_PROPERTY_KEY, testHelper.realm)
                      .post(Entity.json(newRole))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }
              CatalogGrant grantResource =
                  new CatalogGrant(
                      CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
              try (Response response =
                  testHelper
                      .client
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/admin/grants",
                              testHelper.localPort, catalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + testHelper.adminToken)
                      .header(REALM_PROPERTY_KEY, testHelper.realm)
                      .put(Entity.json(grantResource))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }

              try (Response response =
                  testHelper
                      .client
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/admin",
                              testHelper.localPort, catalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + testHelper.adminToken)
                      .header(REALM_PROPERTY_KEY, testHelper.realm)
                      .get()) {
                assertThat(response)
                    .returns(Response.Status.OK.getStatusCode(), Response::getStatus);
                CatalogRole catalogRole = response.readEntity(CatalogRole.class);
                try (Response ignore =
                    testHelper
                        .client
                        .target(
                            String.format(
                                "http://localhost:%d/api/management/v1/principal-roles/catalog-admin/catalog-roles/%s",
                                testHelper.localPort, catalogName))
                        .request("application/json")
                        .header("Authorization", "Bearer " + testHelper.adminToken)
                        .header(REALM_PROPERTY_KEY, testHelper.realm)
                        .put(Entity.json(catalogRole))) {
                  assertThat(response)
                      .returns(Response.Status.OK.getStatusCode(), Response::getStatus);
                }
              }

              SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
              this.restCatalog =
                  new RESTCatalog(
                      context,
                      (config) ->
                          HTTPClient.builder(config)
                              .uri(config.get(CatalogProperties.URI))
                              .build());
              this.restCatalog.initialize(
                  "polaris",
                  ImmutableMap.of(
                      CatalogProperties.URI,
                      "http://localhost:" + testHelper.localPort + "/api/catalog",
                      OAuth2Properties.CREDENTIAL,
                      testHelper.snowmanCredentials.clientId()
                          + ":"
                          + testHelper.snowmanCredentials.clientSecret(),
                      OAuth2Properties.SCOPE,
                      BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL,
                      CatalogProperties.FILE_IO_IMPL,
                      "org.apache.iceberg.inmemory.InMemoryFileIO",
                      "warehouse",
                      catalogName,
                      "header." + REALM_PROPERTY_KEY,
                      testHelper.realm));
            });
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected org.apache.iceberg.catalog.Catalog tableCatalog() {
    return restCatalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return true;
  }
}
