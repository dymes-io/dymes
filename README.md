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

## Quickstart

```sh
make oci
podman run --rm -p 6510:6510 -it dymes:latest
```

## Developing

See [Hacking](./HACKING.md) for information on developing Dymes, bearing in mind the [Dymes Conventions](CONVENTIONS.md).

## Using

### Pre-requisites

#### Podman/Docker

##### macOS

- Podman/Docker Desktop
- Use `containerd`

##### Linux

- Podman or Docker Engine with `containerd`

### Building and running

#### Building the OCI image

```sh
make oci
```

#### Running the OCI image

```sh
podman run --rm -v ./zig-out/dymes-data:/var/dymes/data -p 6510:6510 -it dymes:latest
```

### Important concepts for usage

See [Dymes Usage Concepts](./doc/dymes-usage-concepts.md)

## Roadmap

> PLACEHOLDER

## Dependencies

Dymes has [a few dependencies on third-party libraries](./DEPENDENCIES.md), which are all
open source under OSI-approved licenses.
