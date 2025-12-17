<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Dymes HTTP Interaction

This document describes the main HTTP interactions between a Dymes client and server node.

<!-- TOC -->
* [Dymes HTTP Interaction](#dymes-http-interaction)
* [Message creation](#message-creation)
  * [Append message](#append-message)
* [Message retrieval](#message-retrieval)
  * [Query message](#query-message)
  * [Cursor queries](#cursor-queries)
<!-- TOC -->

# Message creation

## Append message

Stores a message permanently, while ensuring a global ordering of messages.

```mermaid
---
title: Dymes HTTP Message Creation
config:
  look: handDrawn
  theme: neutral
---

sequenceDiagram

    participant client as HTTP Client
    participant server as HTTP Server
    participant engine as Dymes Engine

    client->>server: POST /api/v1/messages [CreationRequest (json)]

    activate server

    server->>engine: Creation request

    activate engine

    engine->>engine: Generate message id (ULID)
    engine->>engine: Append message

    engine->>server: Creation response

    deactivate engine

    alt Success
        server->>client: 201 [Message Id (ULID)]
    else UsageError
        server->>client: 400 [Error]
    else Failure
        server->>client: 500 [Error]
    end

    deactivate server

```

# Message retrieval

## Query message

Allows retrieval of a single message from Dymes.

```mermaid
---
title: Dymes HTTP Single Message Queries
config:
  look: handDrawn
  theme: neutral
---

sequenceDiagram

    participant client as HTTP Client
    participant server as HTTP Server
    participant engine as Dymes Engine

    client->>server: GET /api/v1/messages/{message_id}

    activate server

    server->>server: Query message by id

    activate engine

    engine->>server: Message id

    deactivate engine

    alt Found
        server->>client: 200 [Message]
    else NotFound
        server->>client: 404
    else UsageError
        server->>client: 400 [Error]
    else Failure
        server->>client: 500 [Error]
    end

    deactivate server
```

## Cursor queries

Allows retrieveal of a range of messages from Dymes.

```mermaid
---
title: Dymes HTTP Cursor Queries
config:
  look: handDrawn
  theme: neutral
---

sequenceDiagram

    participant client as HTTP Client
    participant server as HTTP Server
    participant engine as Dymes Engine

    %%--------------------
    %% Open cursor
    %%--------------------

    client->>server: POST /api/v1/cursors [ClientQueryDto (json)]

    activate server

    server->>engine: Open cursor [ClientQuery]

    activate engine

    create participant cursor as Cursor (cursor_id)

    engine->>cursor: Open with query

    engine->>server: cursor_id

    deactivate engine

    alt Cursor created
        server->>client: 201 [cursor_id]
    else UsageError
        server->>client: 400 [Error]
    else Failure
        server->>client: 500 [Error]
    end

    deactivate server

    %%--------------------
    %% Traverse cursor
    %%--------------------


    client->>server: GET /api/v1/cursors/{cursor_id}
    note over server: `batch_size` limits results

    alt Found cursor
        server-->>client: 200 [Chunked]
    else UsageError
        server->>client: 400 [Error]
    else Failure
        server->>client: 500 [Error]
    end

    activate server

    server->>cursor: Traverse cursor

    activate cursor
    loop While message count < `batch_size`
        cursor->>cursor: Next

        cursor->>server: [Message]

        server-->>client: [Chunk (message)]
    end
    deactivate cursor

    deactivate server


    %%--------------------
    %% Close cursor
    %%--------------------


    client->>server: DEL /api/v1/cursors/{cursor_id}

    activate server

    destroy cursor
    server -x cursor: Close cursor

    alt Cursor closed
        server->>client: 200
    else UsageError
        server->>client: 400 [Error]
    else Failure
        server->>client: 500 [Error]
    end

    deactivate server

```
