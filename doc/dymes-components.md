<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Dymes Components

```mermaid
mindmap
Dymes
    Libraries
        dymes_engine["Engine"]
        dymes_http["HTTP"]
        dymes_vsr["VSR"]
        dymes_client["Client"]
        dymes_msg["Message"]
        dymes_msg_store["Message Store"]
        dymes_common["Common"]
    Command line tooling
        dymes_dde["Data Exporter"]
        dymes_ddi["Data Importer"]
        dymes_vsr_sim["VSR Simulator"]
        dymes_workshop["Workshop"]
        dymes_stress["Stress Tool"]
    Service Daemons
        dymes_node["Node"]
        dymes_dme["Metrics Exporter"]
```

<!-- TOC -->
* [Dymes Components](#dymes-components)
  * [Dymes Libraries](#dymes-libraries)
    * [Common library](#common-library)
    * [Message library](#message-library)
    * [Message store library](#message-store-library)
    * [Engine library](#engine-library)
    * [HTTP library](#http-library)
    * [Client library](#client-library)
    * [VSR library](#vsr-library)
  * [Dymes CLI Tooling](#dymes-cli-tooling)
    * [Data Exporter](#data-exporter)
    * [Data Importer](#data-importer)
    * [VSR Simulator](#vsr-simulator)
    * [Workshop](#workshop)
    * [Stress Tool](#stress-tool)
  * [Dymes Service Daemons](#dymes-service-daemons)
    * [Node](#node)
    * [Metrics Exporter](#metrics-exporter)
<!-- TOC -->

---
## Dymes Libraries

The Dymes libraries are intended to be as self-contained as possible while remaining loosely coupled. This allows library mixing-and-matching as required.
 
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


| Library                                 | Description                                                                                           |
|-----------------------------------------|-------------------------------------------------------------------------------------------------------|
| [Common](#common-library)               | Domain-agnostic data structures and utility functions                                                 |
| [Message](#message-library)             | Defines message limits, headers, substructures and builders                                           |
| [Message Store](#message-store-library) | Defines storage limits, data segments, datasets, file formats (data, index and journal)               |
| [Engine](#engine-library)               | Binds the append and immutable stores together, exposing ingress and query functionality              |
| [HTTP](#http-library)                   | An HTTP server with endpoints bound to health, ingress and query support                              |
| [Client](#client-library)               | A minimal HTTP client for testing purposes                                                            |
| [VSR](#vsr-library)                     | A no-frills implementation of [Viewstamped Replication (Revisited)](http://www.pmg.csail.mit.edu/vr/) |


---
### Common library

The common library (`dymes_common`) contains domain-agnostic data structures and utility functions.

```mermaid
flowchart LR
    dymes_common["Common"]
```


- Common limits, constants, and errors
- Utility functions (checksums, base64, networking, etc.)
- Configuration from yaml, environment, procedural
- Component health check support
- Data structures (In-memory queues, LRU cache, etc.)
- Metrics support


---
### Message library

The message library (`dymes_msg`) defines message limits, headers, substructures and builders.

```mermaid
flowchart LR
    dymes_msg["Message"] --> dymes_common["Common"]
```


- Message limits, constants and headers
- Message structure with builder
- Creation and import request structures and builders
- Range queries, correlation queries, channel queries

---
### Message store library

The message store library (`dymes_msg_store`) defines storage limits, data segments, datasets, file formats (data, index and journal) and more.

```mermaid
flowchart LR
    dymes_msg_store["Message Store"] --> dymes_msg["Message"]
    dymes_msg_store["Message Store"] --> dymes_common["Common"]
```


- Storage limits, constants and errors
- Dataset and segment management
- Message, Index and Journal file structures and I/O routines
- Append request structures and builders
- Range, Channel and Correlation query structures and builders
- Append store (Append ops)
- Immutable store (Query ops)

---
### Engine library

The engine library (`dymes_engine`) binds the append and immutable stores together, exposing ingress and query functionality.

```mermaid
flowchart TD
    dymes_engine["Engine"] --> dymes_msg_store["Message Store"]
    dymes_engine["Engine"] --> dymes_msg["Message"]
    dymes_engine["Engine"] --> dymes_common["Common"]
```


- Message ingestion (append and/or import)
- Queries
  - Range
  - Channel
  - Correlation

---
### HTTP library

The HTTP library (`dymes_http`) provides an HTTP server with endpoints bound to health, ingress and query support.

```mermaid
flowchart TD
    dymes_http["HTTP"] --> dymes_engine["Engine"]
    dymes_http["HTTP"] --> dymes_msg["Message"]
    dymes_http["HTTP"] --> dymes_common["Common"]
```


---
### Client library

The client library (`dymes_client`) provides a minimal HTTP client for testing purposes.

```mermaid
flowchart TD
    dymes_client["Client"] --> dymes_msg["Message"]
    dymes_client["Client"] --> dymes_common["Common"]
```


Currently, the client library provides very few guardrails and no retry functionality, since the client will be
replaced entirely once VSR support is complete.

---
### VSR library

The VSR library (`dymes_vsr`) provides a no-frills implementation of [Viewstamped Replication (Revisited)](http://www.pmg.csail.mit.edu/vr/).

```mermaid
flowchart TD
    dymes_vsr["VSR"] --> dymes_msg["Message"]
    dymes_vsr["VSR"] --> dymes_common["Common"]
```


- VSR constants and limits
- Structures for VSR operations, message, and state
- Transport for VSR messaging
- Non-blocking state machine
- Minimal VSR client proxy

> This is under active development and not yet complete (missing support for `GetState` and `NewState`)


---
## Dymes CLI Tooling

Command line tooling for operations and development.

```mermaid
block-beta
  columns 4
    dymes_dde["Data Exporter"]
    dymes_ddi["Data Importer"]
    dymes_vsr_sim["VSR Simulator"]
    dymes_workshop["Workshop"]
    dymes_stress["Stress Tool"]
```


- Data export and import (with a bit of scripting, allows backup/restore)
- Development and testing tools

---
### Data Exporter

Exports query results to a `JSON` file containing import request `DTOs`.

```mermaid
flowchart TD
    dymes_dde["Data Exporter"] --> dymes_common["Common"]
    dymes_dde["Data Exporter"] --> dymes_msg["Message"]
    dymes_dde["Data Exporter"] --> dymes_client["Client"]
```


---
### Data Importer

Imports messages from a `JSON` file containing import request `DTOs`.

```mermaid
flowchart TD
    dymes_ddi["Data Importer"] --> dymes_common["Common"]
    dymes_ddi["Data Importer"] --> dymes_msg["Message"]
    dymes_ddi["Data Importer"] --> dymes_client["Client"]
```


---
### VSR Simulator

Development tool used to set up and test VSR cluster scenarios and manual integration testing.

```mermaid
flowchart TD
    dymes_vsr_sim["VSR Simulator"] --> dymes_vsr["VSR"]
    dymes_vsr_sim["VSR Simulator"] --> dymes_engine["Engine"]
    dymes_vsr_sim["VSR Simulator"] --> dymes_msg["Message"]
    dymes_vsr_sim["VSR Simulator"] --> dymes_common["Common"]
```



---
### Workshop

Development tool used for experimentation and manual integration testing of non-VSR components.

```mermaid
flowchart TD
    dymes_workshop["Workshop"] --> dymes_engine["Engine"]
    dymes_workshop["Workshop"] --> dymes_http["HTTP"]
    dymes_workshop["Workshop"] --> dymes_msg["Message"]
    dymes_workshop["Workshop"] --> dymes_common["Common"]
```


---
### Stress Tool

Testing tool used to generate load for manual testing.

```mermaid
flowchart TD
    dymes_stress["Stress Tool"] --> dymes_client["Client"]
    dymes_stress["Stress Tool"] --> dymes_msg["Message"]
    dymes_stress["Stress Tool"] --> dymes_common["Common"]
```


---
## Dymes Service Daemons

These daemons provide the Dymes executable runtime environment.

```mermaid
block-beta
  columns 4
    dymes_node["Node"]
    dymes_dme["Metrics Exporter"]
```


---
### Node

Functions as a node in a Dymes cluster.

```mermaid
flowchart TD
    dymes_node["Node"] --> dymes_engine["Engine"]
    dymes_node["Node"] --> dymes_http["HTTP"]
    dymes_node["Node"] --> dymes_msg["Message"]
    dymes_node["Node"] --> dymes_common["Common"]
```

---
### Metrics Exporter

```mermaid
flowchart TD
    dymes_dme["Metrics Exporter"] --> dymes_common["Common"]
```

Exports metrics to a [Prometheus](https://prometheus.io) (or [compatible](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/prometheusreceiver)) collector.

> This daemon is launched as a child of a [Node](#node) process.
> 