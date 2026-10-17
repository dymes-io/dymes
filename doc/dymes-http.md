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
  * [Append messages in a batch](#append-messages-in-a-batch)
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

## Append messages in a batch

Stores many messages in one request, in order, under one hold of the engine and with one synchronization with the
filesystem at the end. The answer comes after the synchronization: `201` means every message in it is durable, as for
a single append. The design is in [dymes-batch-append-proposal.md](dymes-batch-append-proposal.md).

`POST /api/v1/messages/batch`, `application/json`:

```json
{
  "messages": [
    { "channel": 5, "routing": 1, "encoded_msg_body": "bWFya2Vy" },
    { "channel": 5, "routing": 2, "kv_entries": [{ "key": "k", "value": "v" }],
      "encoded_msg_body": "cmVjb3Jk", "correlation_index": 0 }
  ]
}
```

- Each entry is a creation request (`encoded_correlation_id`, `channel`, `routing`, `kv_entries`, `encoded_msg_body`).
  `encoded_correlation_id` and `kv_entries` may be left out.
- **`correlation_index`**: correlates to an earlier entry of the same batch, whose id is assigned in the batch. An entry
  gives at most one of `correlation_index` and `encoded_correlation_id`.
- **Limits:** 1 to 1,000 messages; a request body of at most 8 MiB (which also bounds a single message's request).
  A message whose frame exceeds 2 MiB is refused with `400`, on this endpoint (before anything is stored) and on
  `POST /api/v1/messages`.

Answers:

| Status | Body | When |
| --- | --- | --- |
| `201` | `{"ids": ["01M…", …]}` | Stored and synchronized. One id per entry, in request order, consecutive in the global order |
| `400` | the error name | Invalid JSON, out of limits, a `correlation_index` not pointing at an earlier entry, an `encoded_correlation_id` not stored, or both given. Nothing is stored |
| `500` | `{"error": "…", "ids": ["01M…", …]}` | Storage failed part-way. The listed messages, stored before the failure, stay (the store is append-only), so a client can repair. If the final synchronization fails, no ids are listed: the messages may not be on disk, and the node needs repair |
| `501` | the error name | A clustered node: VSR has no batch operation yet |

```mermaid
---
title: Dymes HTTP Batch Message Creation
config:
  look: handDrawn
  theme: neutral
---

sequenceDiagram

    participant client as HTTP Client
    participant server as HTTP Server
    participant engine as Dymes Engine

    client->>server: POST /api/v1/messages/batch [BatchCreationRequest (json)]

    activate server

    server->>engine: Batch of creation requests

    activate engine

    engine->>engine: Check every entry
    loop each entry, in order
        engine->>engine: Generate message id (ULID)
        engine->>engine: Store message (not synchronized)
    end
    engine->>engine: Synchronize with the filesystem, once

    engine->>server: Message ids

    deactivate engine

    alt Success
        server->>client: 201 [Message ids (JSON)]
    else UsageError
        server->>client: 400 [Error]
    else Storage failure part-way
        server->>client: 500 [Error, ids stored (JSON)]
    else Clustered node
        server->>client: 501 [Error]
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
