<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Dymes

Distributed Yet Minimal Event Sourcing

## Overview

Dymes is a flexible, minimalist tool for message wrangling.

It can be used as a commit log, a source of commands, a sink for events, wherever one needs the ability
to reliably store messages/events/commands and be able to query those by various attributes.

All this without having to set up and configure various supporting services.

Deploy via Helm chart into a Kubernetes cluster using minimal configuration, or using Podman/Docker compose for
local development.

## Quickstart (Building and running a Dymes node)

Install the required [developer tooling](./doc/dev-tooling.md), then:

## Build and run a Dymes node locally

```sh
zig build run
```

## Run the Dymes stress client

```sh
zig build stress
```

## Developing

See [Hacking](./HACKING.md) for information on developing Dymes, bearing in mind the [Dymes Conventions](CONVENTIONS.md).

## Roadmap

### Standalone

Working.

- [x] Standalone Node
- [x] Exporter (backup)
- [x] Importer (restore)
- [x] Append messages using HTTP
- [x] Query messages using HTTP
  - [x] Range queries 
  - [x] Correlation queries
  - [x] Channel queries
- [x] Result cursor traversal 

### Clustered

In progress.

- [ ] Clustered Node
- [ ] Append messages using VSR
  - [x] VSR Wire Protocol
  - [x] VSR Transport (Zero MQ)
  - [x] VSR Messages
  - [x] VSR Operations
  - [x] VSR Client Proxy (internal)
  - [ ] VSR State Machine
    - [x] VSR Request
    - [x] VSR Prepare
    - [x] VSR Prepare-OK
    - [x] VSR Commit
    - [x] VSR Start-View-Change
    - [x] VSR Do-View-Change
    - [x] VSR Start-View
    - [ ] VSR Get-State
    - [ ] VSR New-State
  - [ ] HTTP redirect on view change

## Dependencies

Dymes has [a few dependencies on third-party libraries](./DEPENDENCIES.md), which are all
open source under OSI-approved licenses.
