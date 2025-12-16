<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Dymes Subsystems

The Dymes subsystems can be broadly categorized as follows:

- [Libraries](#dymes-libraries)
- [Command line tooling](#dymes-cli-tooling)
- [Service Daemons](#dymes-service-daemons)


---
## Dymes Libraries
 
```mermaid
block-beta
  columns 4
    dymes_engine["Engine"]
    dymes_http["HTTP"]
    dymes_vsr["VSR"]
    dymes_client["Client"]
    dymes_msg["Message"]:2
    dymes_msg_store["Message Store"]:2
    dymes_common["Common"]:4

```

The Dymes libraries are intended to be as self-contained as possible while remaining loosely coupled. This allows library mixing-and-matching as required.

| Library                                 | Description                                                                              |
|-----------------------------------------|------------------------------------------------------------------------------------------|
| [Common](#common-library)               | Domain-agnostic data structures and utility functions                                    |
| [Message](#message-library)             | Defines message limits, headers, substructures and builders                              |
| [Message Store](#message-store-library) | Defines storage limits, data segments, datasets, file formats (data, index and journal)  |
| [Engine](#engine-library)               | Binds the append and immutable stores together, exposing ingress and query functionality |
| [HTTP](#http-library)                   | An HTTP server with endpoints bound to health, ingress and query support                 |
| [Client](#client-library)               | A minimal HTTP client for testing purposes                                               |
| [VSR](#vsr-library)                     | A no-frills implementation of Viewstamped Replication (Revisited)                        |


---
### Common library

```mermaid
flowchart LR
    dymes_common["Common"]
```

The common library (`dymes_common`) contains domain-agnostic data structures and utility functions.

- Common limits, constants, and errors
- Utility functions (checksums, base64, networking, etc.)
- Configuration from yaml, environment, procedural
- Component health check support
- Data structures (In-memory queues, LRU cache, etc.)
- Metrics support


---
### Message library

```mermaid
flowchart LR
    dymes_msg["Message"] --> dymes_common["Common"]
```

The message library (`dymes_msg`) defines message limits, headers, substructures and builders.

- Message limits, constants and headers
- Message structure with builder
- Creation and import request structures and builders
- Range queries, correlation queries, channel queries

---
### Message store library

```mermaid
flowchart LR
    dymes_msg_store["Message Store"] --> dymes_msg["Message"]
    dymes_msg_store["Message Store"] --> dymes_common["Common"]
```

The message store library (`dymes_msg_store`) defines storage limits, data segments, datasets, file formats (data, index and journal) and more. 

- Storage limits, constants and errors
- Dataset and segment management
- Message, Index and Journal file structures and I/O routines
- Append request structures and builders
- Range, Channel and Correlation query structures and builders
- Append store (Append ops)
- Immutable store (Query ops)

---
### Engine library

```mermaid
flowchart TD
    dymes_engine["Engine"] --> dymes_msg_store["Message Store"]
    dymes_engine["Engine"] --> dymes_msg["Message"]
    dymes_engine["Engine"] --> dymes_common["Common"]
```


The engine library (`dymes_engine`) binds the append and immutable stores together, exposing ingress and query functionality.

- Message ingestion (append and/or import)
- Queries
  - Range
  - Channel
  - Correlation

---
### HTTP library

```mermaid
flowchart TD
    dymes_http["HTTP"] --> dymes_engine["Engine"]
    dymes_http["HTTP"] --> dymes_msg["Message"]
    dymes_http["HTTP"] --> dymes_common["Common"]
```

The HTTP library (`dymes_http`) provides an HTTP server with endpoints bound to health, ingress and query support.

---
### Client library

```mermaid
flowchart TD
    dymes_client["Client"] --> dymes_msg["Message"]
    dymes_client["Client"] --> dymes_common["Common"]
```

The client library (`dymes_client`) provides a minimal HTTP client for testing purposes.

Currently, the client library provides very few guardrails and no retry functionality, since the client will be
replaced entirely once VSR support is complete.

---
### VSR library

```mermaid
flowchart TD
    dymes_vsr["VSR"] --> dymes_msg["Message"]
    dymes_vsr["VSR"] --> dymes_common["Common"]
```

The VSR library (`dymes_vsr`) provides a no-frills implementation of Viewstamped Replication (Revisited).

> This is under active development and not yet complete (missing support for `GetState` and `NewState`)

- VSR constants and limits
- Structures for VSR operations, message, and state
- Transport for VSR messaging
- Non-blocking state machine
- Minimal VSR client proxy

---
## Dymes CLI Tooling

```mermaid
block-beta
  columns 4
    dymes_dde["Data Exporter"]
    dymes_ddi["Data Importer"]
    dymes_vsr_sim["VSR Simulator"]
    dymes_workshop["Workshop"]
    dymes_stress["Stress Tool"]
```

Command line tooling for operations and development.

- Data export and import (with a bit of scripting, allows backup/restore)
- Development and testing tools

---
### Data Exporter

```mermaid
flowchart TD
    dymes_dde["Data Exporter"] --> dymes_common["Common"]
    dymes_dde["Data Exporter"] --> dymes_msg["Message"]
    dymes_dde["Data Exporter"] --> dymes_client["Client"]
```

Exports query results to a `JSON` file containing import request `DTOs`.

---
### Data Importer

```mermaid
flowchart TD
    dymes_ddi["Data Importer"] --> dymes_common["Common"]
    dymes_ddi["Data Importer"] --> dymes_msg["Message"]
    dymes_ddi["Data Importer"] --> dymes_client["Client"]
```


Imports messages from a `JSON` file containing import request `DTOs`.

---
### VSR Simulator

```mermaid
flowchart TD
    dymes_vsr_sim["VSR Simulator"] --> dymes_vsr["VSR"]
    dymes_vsr_sim["VSR Simulator"] --> dymes_engine["Engine"]
    dymes_vsr_sim["VSR Simulator"] --> dymes_msg["Message"]
    dymes_vsr_sim["VSR Simulator"] --> dymes_common["Common"]
```


Development tool used to set up and test VSR cluster scenarios and manual integration testing.

---
### Workshop

```mermaid
flowchart TD
    dymes_workshop["Workshop"] --> dymes_engine["Engine"]
    dymes_workshop["Workshop"] --> dymes_http["HTTP"]
    dymes_workshop["Workshop"] --> dymes_msg["Message"]
    dymes_workshop["Workshop"] --> dymes_common["Common"]
```

Development tool used for experimentation and manual integration testing of non-VSR components.  

---
### Stress Tool

```mermaid
flowchart TD
    dymes_stress["Stress Tool"] --> dymes_client["Client"]
    dymes_stress["Stress Tool"] --> dymes_msg["Message"]
    dymes_stress["Stress Tool"] --> dymes_common["Common"]
```

Testing tool used to generate load for manual testing.

---
## Dymes Service Daemons

```mermaid
block-beta
  columns 4
    dymes_node["Node"]
    dymes_dme["Metrics Exporter"]
```

These daemons provide the Dymes executable runtime environment.

---
### Node

```mermaid
flowchart TD
    dymes_node["Node"] --> dymes_engine["Engine"]
    dymes_node["Node"] --> dymes_http["HTTP"]
    dymes_node["Node"] --> dymes_msg["Message"]
    dymes_node["Node"] --> dymes_common["Common"]
```

Functions as a node in a Dymes cluster.

---
### Metrics Exporter

```mermaid
flowchart TD
    dymes_dme["Metrics Exporter"] --> dymes_common["Common"]
```

Exports metrics to a [Prometheus](https://prometheus.io) (or [compatible](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/prometheusreceiver)) collector.

> This daemon is launched as a child of a [Node](#node) process.