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
package org.apache.polaris.extension.auth.syncope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Syncope-based implementation of {@link PolarisAuthorizer}.
 *
 * <p>This authorizer delegates authorization decisions to an Apache Syncope instance by querying
 * the Syncope REST API. It retrieves the entitlements assigned to the principal and checks whether
 * the required entitlement for the requested operation is present.
 *
 * <p>The mapping between Polaris operations and Syncope entitlements uses the operation name
 * directly (e.g., {@code CATALOG_MANAGE_CONTENT}, {@code TABLE_READ_DATA}).
 */
class SyncopePolarisAuthorizer implements PolarisAuthorizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncopePolarisAuthorizer.class);

  private final URI baseUri;
  private final String domain;
  private final String authHeader;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;

  SyncopePolarisAuthorizer(
      @Nonnull URI baseUri,
      @Nonnull String domain,
      @Nonnull String username,
      @Nonnull String password,
      @Nonnull CloseableHttpClient httpClient,
      @Nonnull ObjectMapper objectMapper) {
    this.baseUri = baseUri;
    this.domain = domain;
    this.authHeader =
        "Basic "
            + Base64.getEncoder()
                .encodeToString(
                    (username + ":" + password).getBytes(StandardCharsets.UTF_8));
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
  }

  @Override
  public void resolveAuthorizationInputs(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    throw new UnsupportedOperationException(
        "resolveAuthorizationInputs is not implemented yet for SyncopePolarisAuthorizer");
  }

  @Override
  public AuthorizationDecision authorize(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    throw new UnsupportedOperationException(
        "authorize is not implemented yet for SyncopePolarisAuthorizer");
  }

  @Override
  public void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary) {
    authorizeOrThrow(
        polarisPrincipal,
        activatedEntities,
        authzOp,
        target == null ? null : List.of(target),
        secondary == null ? null : List.of(secondary));
  }

  @Override
  public void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {
    String principalName = polarisPrincipal.getName();
    String requiredEntitlement = authzOp.name();

    Set<String> entitlements = fetchUserEntitlements(principalName);
    if (!entitlements.contains(requiredEntitlement)) {
      LOGGER.debug(
          "Syncope denied authorization for principal '{}': missing entitlement '{}'",
          principalName,
          requiredEntitlement);
      throw new ForbiddenException(
          "Syncope denied authorization: principal '%s' lacks entitlement '%s'",
          principalName, requiredEntitlement);
    }
  }

  /**
   * Fetches entitlements for a user from the Syncope REST API.
   *
   * <p>Calls the Syncope user endpoint to retrieve the user's details and extracts the entitlements
   * from the response.
   *
   * @param username the Syncope username to look up
   * @return the set of entitlement names assigned to the user
   * @throws RuntimeException if the Syncope query fails
   */
  private Set<String> fetchUserEntitlements(String username) {
    try {
      // Build the Syncope REST API URL to get user details
      String userUri = baseUri.toString().replaceAll("/+$", "") + "/rest/users/" + username;
      HttpGet httpGet = new HttpGet(URI.create(userUri));
      httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
      httpGet.setHeader(HttpHeaders.ACCEPT, "application/json");
      httpGet.setHeader("X-Syncope-Domain", domain);

      return httpClient.execute(
          httpGet,
          response -> {
            int statusCode = response.getCode();
            if (statusCode != 200) {
              LOGGER.warn(
                  "Syncope returned status {} when fetching user '{}'", statusCode, username);
              return Set.of();
            }

            String responseBody;
            try {
              responseBody = EntityUtils.toString(response.getEntity());
            } catch (ParseException e) {
              throw new RuntimeException("Failed to parse Syncope response", e);
            }

            return parseEntitlements(responseBody);
          });
    } catch (IOException e) {
      throw new RuntimeException("Syncope query failed for user: " + username, e);
    }
  }

  /**
   * Parses entitlements from a Syncope user JSON response.
   *
   * <p>Syncope returns user entitlements in the {@code entitlements} array field of the user
   * resource.
   *
   * @param responseBody the JSON response body from Syncope
   * @return the set of entitlement names
   */
  private Set<String> parseEntitlements(String responseBody) {
    try {
      JsonNode root = objectMapper.readTree(responseBody);
      Set<String> entitlements = new HashSet<>();

      // Syncope user response contains an "entitlements" array
      JsonNode entitlementsNode = root.path("entitlements");
      if (entitlementsNode.isArray()) {
        for (JsonNode entitlement : entitlementsNode) {
          entitlements.add(entitlement.asText());
        }
      }

      return entitlements;
    } catch (IOException e) {
      LOGGER.warn("Failed to parse Syncope entitlements response", e);
      return Set.of();
    }
  }
}
