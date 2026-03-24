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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.Timeout;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for creating Apache Syncope-based Polaris authorizer implementations. */
@ApplicationScoped
@Identifier("syncope")
class SyncopePolarisAuthorizerFactory implements PolarisAuthorizerFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SyncopePolarisAuthorizerFactory.class);

  private final SyncopeAuthorizationConfig syncopeConfig;
  private final ObjectMapper objectMapper;
  private CloseableHttpClient httpClient;

  @Inject
  public SyncopePolarisAuthorizerFactory(SyncopeAuthorizationConfig syncopeConfig) {
    this.syncopeConfig = syncopeConfig;
    this.objectMapper = JsonMapper.builder().build();
  }

  SyncopeAuthorizationConfig getConfig() {
    return syncopeConfig;
  }

  @PostConstruct
  public void initialize() {
    syncopeConfig.validate();
    httpClient = createHttpClient();
  }

  @Override
  public PolarisAuthorizer create(RealmConfig realmConfig) {
    URI baseUri =
        syncopeConfig
            .baseUri()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Syncope base URI must be configured via polaris.authorization.syncope.base-uri"));

    String username =
        syncopeConfig
            .username()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Syncope username must be configured via polaris.authorization.syncope.username"));

    String password =
        syncopeConfig
            .password()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Syncope password must be configured via polaris.authorization.syncope.password"));

    return new SyncopePolarisAuthorizer(
        baseUri, syncopeConfig.domain(), username, password, httpClient, objectMapper);
  }

  @PreDestroy
  public void cleanup() {
    if (httpClient != null) {
      try {
        httpClient.close();
        LOGGER.debug("HTTP client closed successfully");
      } catch (IOException e) {
        LOGGER.warn("Error closing HTTP client: {}", e.getMessage(), e);
      }
    }
  }

  private CloseableHttpClient createHttpClient() {
    SyncopeAuthorizationConfig.HttpConfig httpConfig = syncopeConfig.http();
    Timeout timeout = Timeout.ofMilliseconds(httpConfig.timeout().toMillis());

    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectionRequestTimeout(timeout)
            .setResponseTimeout(timeout)
            .build();

    return HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
  }
}
