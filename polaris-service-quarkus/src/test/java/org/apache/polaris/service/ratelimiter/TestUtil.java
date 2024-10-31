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
package org.apache.polaris.service.ratelimiter;

import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.function.Consumer;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;

/** Common test utils for testing rate limiting */
public class TestUtil {
  public static Consumer<Status> constructRequestAsserter(PolarisIntegrationTestHelper testHelper) {
    return (Response.Status status) -> {
      try (Response response =
          testHelper
              .client
              .target(
                  String.format(
                      "http://localhost:%d/api/management/v1/principal-roles",
                      testHelper.localPort))
              .request("application/json")
              .header("Authorization", "Bearer " + testHelper.adminToken)
              .header(REALM_PROPERTY_KEY, testHelper.realm)
              .get()) {
        assertThat(response).returns(status.getStatusCode(), Response::getStatus);
      }
    };
  }
}
