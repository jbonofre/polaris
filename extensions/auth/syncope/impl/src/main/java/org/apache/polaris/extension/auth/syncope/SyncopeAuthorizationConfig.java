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

import static com.google.common.base.Preconditions.checkArgument;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Configuration for Apache Syncope authorization.
 *
 * <p>Apache Syncope is an open-source system for managing digital identities. This configuration
 * allows Polaris to delegate authorization decisions to a Syncope instance by querying its REST API
 * for user entitlements.
 */
@PolarisImmutable
@ConfigMapping(prefix = "polaris.authorization.syncope")
public interface SyncopeAuthorizationConfig {

  /** Base URI of the Apache Syncope instance (e.g., {@code https://syncope.example.com/syncope}). */
  Optional<URI> baseUri();

  /** The Syncope domain to use for API requests. Defaults to {@code Master}. */
  @WithDefault("Master")
  String domain();

  /** Admin username for Syncope REST API authentication. */
  Optional<String> username();

  /** Admin password for Syncope REST API authentication. */
  Optional<String> password();

  /** HTTP client configuration for Syncope communication. */
  HttpConfig http();

  /** Validates the complete Syncope configuration. */
  default void validate() {
    checkArgument(
        baseUri().isPresent(), "polaris.authorization.syncope.base-uri must be configured");

    URI uri = baseUri().get();
    String scheme = uri.getScheme();
    checkArgument(
        "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme),
        "polaris.authorization.syncope.base-uri must use http or https scheme, but got: " + scheme);

    checkArgument(
        username().isPresent(), "polaris.authorization.syncope.username must be configured");
    checkArgument(
        password().isPresent(), "polaris.authorization.syncope.password must be configured");
  }

  /** HTTP client configuration for Syncope communication. */
  @PolarisImmutable
  interface HttpConfig {
    @WithDefault("PT5S")
    Duration timeout();

    @WithDefault("true")
    boolean verifySsl();
  }
}
