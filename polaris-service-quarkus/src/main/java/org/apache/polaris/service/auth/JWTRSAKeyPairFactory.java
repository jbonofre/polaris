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
package org.apache.polaris.service.auth;

import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.RequestScoped;
import java.time.Duration;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.config.RuntimeCandidate;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@RequestScoped
@RuntimeCandidate
@LookupIfProperty(
    name = "polaris.authentication.token-broker-factory.type",
    stringValue = "rsa-key-pair")
public class JWTRSAKeyPairFactory implements TokenBrokerFactory {

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final Duration maxTokenGenerationInSeconds;

  public JWTRSAKeyPairFactory(
      MetaStoreManagerFactory metaStoreManagerFactory,
      @ConfigProperty(name = "polaris.authentication.token-broker-factory.max-token-generation")
          Duration maxTokenGenerationInSeconds) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
  }

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    return new JWTRSAKeyPair(
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext),
        (int) maxTokenGenerationInSeconds.toSeconds());
  }
}
