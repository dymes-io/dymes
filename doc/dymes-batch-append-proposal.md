<!--
SPDX-FileCopyrightText: Copyright © 2026 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Proposal: batch append

Status: proposed (2026-10-17).

Append many messages in one request, under one hold of the engine mutex, with
one sync at the end. Today a node's write rate is bounded by one HTTP request,
one mutex turn and two fsyncs per message.

<!-- TOC -->
* [Proposal: batch append](#proposal-batch-append)
  * [The problem, measured](#the-problem-measured)
  * [What a node does today](#what-a-node-does-today)
  * [The endpoint](#the-endpoint)
  * [Engine and store](#engine-and-store)
  * [Alternatives considered](#alternatives-considered)
  * [Clients](#clients)
  * [Tests](#tests)
<!-- TOC -->

## The problem, measured

A standalone node at `72267d3` on AWS (r7i.xlarge, data on an encrypted
200 GiB gp3 volume), written by one client, the volta market-data ingestion
service, in October 2026:

| Client | Messages a second | Appends in flight | Time per batch of ~265 messages | Volume |
| --- | --- | --- | --- | --- |
| One append at a time | about 100 | 1 | about 4 s | 640 IOPS |
| Up to 8 at once | 319 | 3.1 | about 12 s | 1,030 IOPS |
| Up to 8 at once, more work queued | 273 | 5.7 | about 21 s | 980 IOPS |

Past about 3 appends in flight, each append only waited longer. The node used
**3% CPU** and a third of the volume's 3,000 IOPS, each write about 0.85 ms.
Concurrent appends are safe at `72267d3` (8 callers × 300 messages, every
channel intact and in order); they just don't go faster.

## What a node does today

- **One request per message:** `POST /api/v1/messages` takes one
  `CreationRequest` and answers `201` with the ULID as text
  (`src/dymes_http/RequestContext.zig`, `handleCreateMsg`).
- **One writer:** appends, queries and cursor traversals all take
  `Engine.mtx_engine` (`src/dymes_engine/Engine.zig`).
- **Two fsyncs per message:** `AppendStore.append` calls `dataset.sync()`
  after each message (`src/dymes_msg_store/AppendStore.zig`), which at the
  default `SyncLevel.msg_and_journal` syncs the segment's message file and the
  hot journal. The level isn't configurable.
- **No batch anywhere:** not over HTTP, in the engine, in VSR (`VsrOpType`
  has only `append`) or in the clients.
- **Correlations point at stored messages:** `Engine.import` looks the
  correlation id up and rejects the append if it isn't stored yet.

## The endpoint

`POST /api/v1/messages/batch`

### Request

```json
{
  "messages": [
    { "channel": "...", "routing": "...", "kv_entries": [], "encoded_msg_body": "..." },
    { "channel": "...", "routing": "...", "kv_entries": [], "encoded_msg_body": "...",
      "correlation_index": 0 }
  ]
}
```

- Each entry is a `CreationRequest`, plus an optional **`correlation_index`**:
  the position of an **earlier** entry in the same batch to correlate to. Ids
  are assigned by the node, so a client can't give an earlier entry's id. An
  entry gives at most one of `correlation_index` and `encoded_correlation_id`.
- **Limits** (configurable): at most 1,000 messages and 16 MiB a request;
  each message within today's frame limit.

### Response

`201 Created`, `application/json`:

```json
{ "ids": ["01M…", "01M…"] }
```

One ULID per entry, in request order. The batch holds the mutex, so its ids
are consecutive in the node's global order. The answer comes after the sync:
`201` means every message in it is durable, as for a single append.

### Failures

- **Checked before anything is written** (`400`, nothing appended): parse
  errors, limits, a `correlation_index` that isn't an earlier entry, an
  `encoded_correlation_id` that isn't stored.
- **A storage failure part-way through** (`500`): the store is append-only,
  so messages written before the failure stay. The body lists them so the
  client can repair:

  ```json
  { "error": "FailedToStore", "ids": ["01M…"] }
  ```

  As for a single append, a storage failure means the node needs attention.

## Engine and store

- `Engine.appendBatch(entries)`:
  1. lock `mtx_engine` once;
  2. for each entry: assign the next ULID, resolve its correlation (an index
     becomes that entry's new id, already in the index), store it **without**
     syncing, then `query_subsystem.appended`;
  3. `dataset.sync()` once;
  4. unlock.
- `AppendStore` gains a store-without-sync and an explicit sync. A rollover
  part-way through a batch syncs the segment it closes, as rollover does
  today, before writing to the next.
- **Clustered (VSR) nodes:** out of scope for the first version; the endpoint
  answers `501` until VSR has a batch op.
- **Metrics:** batch appends, messages per batch, sync time.

## Alternatives considered

- **A configurable sync level** (e.g. sync every N ms): fewer fsyncs, but
  `201` would no longer mean durable.
- **More concurrency in the engine:** the single writer is what keeps ids in a
  global order; removing it is a far larger change.
- **A faster disk** (instance-store NVMe): about 10× lower write latency, but
  the data is lost when the instance stops.

## Clients

- **Zig client:** `Client.appendMessages`, beside `Client.appendMessage`.
- **dymes-java:** `DymesClient.appendBatch()`, a builder with `add(...)` per
  message, `withCorrelationIndex(int)` and `List<Ulid> commit()`, splitting
  at the limits. A client that gets `404` from an older node falls back to
  single appends.

## Tests

End-to-end, in the style of the `dymes_ddi` tests (an in-process engine, HTTP
server and client):
- order, and consecutive ids;
- correlation by index, and by an existing id;
- the limits;
- a rollover part-way through a batch;
- a validation failure appending nothing;
- a single append and a batch interleaved.

CI doesn't run the tests at present (`make test` is commented out in
`.github/workflows/oci-build-publish.yaml`), so they're run locally and
against a built image before merging.

## Expected gain

The request overhead and the two fsyncs are paid once per batch instead of
once per message. Writing the frames stays per message. On a volume like the
one measured, expect tens of times more messages a second; the
implementation's first benchmark measures it.
