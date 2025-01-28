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

package org.apache.polaris.core.persistence;

import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.exceptions.PolarisException;

import java.util.List;

/**
 * PolarisStore represents all interactions with the store backend. Each PolarisStore implementation is specific to a backend (JDBC, DynamoDB, ...).
 * PolarisStore implementations should not contain any business
 *
 * The purpose is:
 * 1. PolarisMetaStoreManager should implement the business logic and delegates all interaction with the store to PolarisStore impl
 * 2. The PolarisMetaStoreSession could be one implementation of PolarisStore
 * 3. JDBC or NoSQL (DynamoDB, MongoDB, ...) can implement PolarisStore and deal with all store backend specific.
 */
public interface PolarisStore {

    /**
     * Get a catalog entity from the store
     */
    CatalogEntity getCatalog(String name) throws PolarisException;

    /**
     * Create a catalog in the store.
     *
     * @param catalog
     * @param principalRoles
     */
    void createCatalog(CatalogEntity catalog, List<PrincipalRoleEntity> principalRoles) throws PolarisException;

    /**
     * Retrieve an entity (identify by name) from the store.
     *
     * @param name the entity name
     * @param type the entity type
     * @param subType the entity subtype
     * @param catalogs the list of catalogs (path) where the entity is looking for
     * @return the entity.
     */
    PolarisBaseEntity getEntityByName(String name, PolarisEntityType type, PolarisEntitySubType subType, List<CatalogEntity> catalogs) throws PolarisException;

    /**
     * List all entities in the store.
     *
     * @param type the entity type
     * @param subType the entity subtype
     * @param catalogs the list of catalogs (path) for entities.
     * @return the list of entities.
     */
    List<PolarisBaseEntity> listEntities(PolarisEntityType type, PolarisEntitySubType subType, List<CatalogEntity> catalogs) throws PolarisException;

    /**
     * Create a principal entity in the store.
     *
     * @param principal the principal entity.
     */
    void createPrincipal(PrincipalEntity principal) throws PolarisException;

    /**
     * Create or get an entity in the provided catalog paths.
     *
     * @param entity the entity to create.
     * @param catalogs the catalog paths.
     * @return the existing or created entity.
     */
    PolarisBaseEntity getOrCreateEntity(PolarisBaseEntity entity, List<CatalogEntity> catalogs) throws Exception;

    // TODO renameEntity
    // TODO dropEntity
    // TODO 

}
