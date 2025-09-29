---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
linkTitle: Project Charter
type: docs
weight: 500
---

# Polaris Project Charter

## Introduction

Apache Polaris is a catalog platform for data lakes. It provides new levels of choice, flexibility and control over data, with full entreprise security and Apache Iceberg interoperability across a multitude of engines and infrastructure. Polaris builds on standards such as those created by Apache Iceberg, providing the following benefits to the ecosystem:
* Multi-engine interoperability over a single copy of data, eliminating the need of moving and copying data across different engines and catalogs.
* An interoperable security model providing a unified authorization layer independent from the engines processing analytical tables.
* For multi-catalog scenarios, a unified catalog level view of data across multiple catalogs via catalog notification integrations.
* The ability to host Polaris Catalog on the infrastructure of your choice.

## Mission

With Polaris, we believe we can provide a state of the art, open source, vendor neutral Apache Iceberg catalog built as both a production-ready reference implementation, as well as a providing ground for potential new Iceberg Catalog Spec features such as security and data governance.
As of today, Polaris already supports the following key pieces of functionality.
* **Cross-engine read and write interoperability**: Many organizations either use various processing engines to perform specific workloads or seek the flexibility to easily add or swap processing engines in the future. Either way, they want the freedom to safely use multiple engines on a single copy of data to minimize the storage and compute costs associated with moving data or maintaining multiple copies.
    Catalogs play a critical role in a multi-engine architecture. They make operations on tables reliable by supporting atomic transactions. This means that data engineers and their pipelines can modify tables concurrently, and queries on these tables produce accurate results. To accomplish this, all Apache Iceberg table read and write operations, even from different engines, are routed through a catalog.
  Polaris implements the full Apache Iceberg open REST API to maximize the number of engines you can utilize, and integrates credential vending with all major public cloud storage vendors, also with on-prem services.
* **Security**: Polaris implements a brand new role based access control (RBAC) security model, designed from first principles to provide a generalized foundation for fine-grained security. Security is a critical piece of enterprise catalogs, and with Polaris we aim to help push forward the state of the art in OSS catalog security.
* **Multi-tenancy**: Built from the ground up to support serving multiple catalogs and multiple users from a single instance.
* **Run anywhere, no lock-in**: Polaris can be deployed on different infrastructures: cloud infrastructure, your own infrastructure with containers such as Docker or Kubernetes. Regardless of how you deploy Polaris, there’s no lock-in.

Our mission is also to provide the catalog features expected by the community:
* Policies management.
* New permission model support (Fine-Grained Access Control, Attribute Based Access Control, User-Based Access Control, ...).
* Support for non-Iceberg data lakes.
* Support for any source of data (structured, semi-structured, unstructured data).
* ...

## Community & Vision

Our focus is to have a vibrant and healthy Polaris community, both contributors and users:
* https://polaris.apache.org/community/community-guidelines/

The Polaris roadmap and features are discussed in the Polaris community, driven by consensus.

Two typologies of Polaris users are identified:
* The end-users: they expect to use Polaris "out of the box", without addition or plugin, just configured to match their infrastructure.
* The integrator-users: they use Polaris as a platform they can extend (adding their own components), matching their use cases.

Our vision is:
1. Polaris is opinionated by default, make choices to run smoothly for the end-users. The purpose here is not to ship all requested components, but provide a default Polaris distribution, usable by the majority of end-users.
2. Polaris is extensible at core, allowing the integrator-users to easily add their components in Polaris thanks to the provided API, and building their own custom Polaris distribution.

## The ASF Incubator

Apache Polaris (incubating) is an effort undergoing incubation at The Apache
Software Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects.

While incubation status is not necessarily a reflection of the completeness
or stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.

## Podling Project Management Committee

https://incubator.apache.org/guides/ppmc.html

## Committer

https://incubator.apache.org/guides/ppmc.html

## Infrastructure

The Apache Polaris podling relies on the Apache Infrastructure project for the following:
* Issues and CI services
* Source repository
* Website
* Mailing lists

## Licensing

All contributions to the Apache Polaris podling adhere to the "Apache Software Foundation License, Version 2.0" (http://www.apache.org/licenses/LICENSE-2.0).
All further contributions, including patches, must be made under the same terms.

When a committer is considering integrating a contribution from a contributor who has no CLA on file with The ASF, it is the responsibility of the committer, in consultation with the PPMC, to conduct due diligence on the pedigree of the contribution under consideration. 

## Development Process, Voting, and Contribution Guideline

https://polaris.apache.org/community/contributing-guidelines/

## Relationship to other Apache projects

The Apache Polaris podling should work closely with other Apache projects, such as Apache Iceberg, to avoid redundancy and achieve coherent implementations of the specifications.
