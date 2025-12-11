<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->
# Developing Dymes

## Pre-requisites

### zig 0.15.2

See the [Zig Download](https://ziglang.org/download/) page for details.

#### macOS

```sh
brew install zig
```

#### Linux

##### Alpine Linux

```sh
apk add zig
```

### Podman/Docker

#### macOS

- Podman/Docker Desktop
- Use `containerd`

#### Linux

- Podman/Docker Engine with `containerd`

### Local build

#### Build Dymes and run unit tests

```sh
make
```

or

```sh
zig build test -freference-trace --summary all
```


#### Running Dymes locally

```sh
zig build run
```


#### Running Dymes stress client

```sh
zig build stress
```

### Docker


```sh
make oci
```

or

```sh
scripts/build-oci.sh
```

#### Running interactive using the Docker image

```sh
podman run --rm -v ./zig-out/dymes-data:/var/dymes/data -p 6510:6510 -it dymes:latest
```

#### Running as daemon using the Docker image

```sh
podman run --name dymes -v ./zig-out/dymes-data:/var/dymes/data -p 6510:6510 -d dymes:latest
```

#### Building in a pipeline

This builds images for:

| OS     | Processor       | Linkage |
|--------|-----------------|---------|
| linux  | `x86-64/amd64`  | `musl`  |
| linux  | `aarch64/arm64` | `musl`  |


### Dymes HTTP

See [Dymes HTTP](./doc/dymes-http.md) for a description of interactions between a Dymes client and server node.

