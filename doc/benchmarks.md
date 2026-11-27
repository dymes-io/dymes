<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

`dymes_bench` builds a reproducible store of a chosen size and drives a running node with
scenarios that each measure one thing a deployment depends on. Use it to reproduce a problem
before fixing it, and to show the numbers before and after.

## A store

```sh
zig build -Doptimize=ReleaseSafe install
zig-out/bin/dymes_bench gen --dir /data/bench/full --manifest /data/bench/full.json --messages 66000000
```

`gen` writes through the message store library, about 250,000 messages a second, so 66 million
messages (about 100 GiB in 252 segments) take about five minutes. The same `--seed`,
`--messages`, `--channels`, `--days` and `--end-ms` give the same store.

| Messages | Size | Segments | Use |
| --- | --- | --- | --- |
| 1,000,000 | 1.6 GiB | 4 | a quick check of the harness |
| 6,600,000 | 10 GiB | 26 | the small end of the size comparison |
| 66,000,000 | 107 GiB | 252 | production scale |

The store is shaped like a feed-ingestion deployment's:

- **Skewed channels.** Of 160 channels one holds a third of the messages, seven more hold
  another third, and about a hundred hold dozens to a few thousand each.
- **Runs.** Messages arrive in runs: a marker, records correlated to the marker, and a closing
  message. Every message of a run carries the same `tracking_id` header, with eleven more
  headers. Runs of different channels interleave in bursts of up to 64 messages.
- **A sparse routing domain.** About one message in 600,000 is in another routing domain, on its
  own channel.
- **Ids over time.** Message ids spread evenly over `--days` (90) up to `--end-ms` (a minute
  ago), so id ranges mean time ranges.

The manifest records what was written: each channel's message count and id range, and three of
its runs (tracking id, id range, message count). The scenarios query from it and check what the
node returns against it.

## Scenarios

```sh
scripts/bench/run.sh /data/bench/full /data/bench/full.json results channel-read tracking-lookup
```

`run.sh` gives each scenario its own copy of the store (a copy-on-write clone where the
filesystem has one), starts a node on it, records how long the node took to become ready, runs
the scenario and stops the node. Results are JSON lines in `results/<scenario>.jsonl`, with the
node's log beside them. Set `DYMES_IMAGE` to run the node from a container image, limited to
four CPUs.

| Scenario | What it does | What a fit node shows |
| --- | --- | --- |
| `channel-read` | Reads a channel of dozens of messages, a mid-sized one and the largest, whole and for the last day. | The time follows the channel's messages, not the store's: a small channel reads in under 100 ms on a store of any size. |
| `tracking-lookup` | Finds a run by its `tracking_id` header on its channel: over the run's own range, over a day, and to the store's end. | Under 1 s on the largest channel over a day. |
| `sparse-routing` | Reads the whole store with a routing filter that matches almost nothing, while another client appends every 20 ms. | Appends take as long during the scan as before it. |
| `mixed-load` | An appender, 23 consumers each following a channel, two lookups by tracking id and a sparse routing consumer, together. | Appends under 100 ms at the 99th percentile and under 1 s at worst. |
| `newest` | A channel query starting at the store's newest message, and channel and range queries whose range ends before it starts. | The message comes back, the inverted ranges are refused, and the node still answers. |
| `rollover` | Appends 300,000 messages on two channels, so the node rolls over to a new segment while serving, then reads both channels and a generated one. | Every message once, in order. |
| `cursor-leak` | Opens 32 cursors, reads a batch from each and goes away without closing them. | No cursor is open once the idle limit has passed. |
| `abandoned-traversal` | Four clients each open a filtered read of the whole store that matches nothing, hang up 200 ms into the fetch and close the cursor from another connection. | A new client is answered promptly once they stop. |
| `hung-up-traversal` | The same, but the clients hang up and never close their cursors. | The same: the node stops reading for a client that has gone. |

## Faults

```sh
scripts/bench/run.sh /data/bench/small /data/bench/small.json results crash disk-full restart
```

These stop, starve or restart the node themselves, so they run a native node only.

| Scenario | What it does | What a fit node shows |
| --- | --- | --- |
| `crash` | Kills the node with `SIGKILL` while four clients append (two a message at a time, two in batches) and another reads a large channel, and restarts it on the same store. `CRASH_ROUNDS` times (3), each check covering every round so far. | Every append the node acknowledged is there, once and in order; the node appends again; a generated channel reads as before. |
| `disk-full` | Fills the store's volume while the same clients append, frees the space, then stops and starts the node. | With the volume full the node refuses appends, says it is not ready and still serves reads. Started again with space, it has every append it acknowledged and appends again. |
| `restart` | Stops the node with `SIGTERM` while eight part-read cursors are open and four clients each have a long fetch in flight, and starts it again. | The node stops within 10 s, and afterwards refuses the old cursors. |

`disk-full` needs a volume about 400 MB larger than the store. On macOS it makes a disk image and
removes it afterwards; elsewhere give it an empty directory on such a volume in `BENCH_SMALL_FS`,
for example `sudo mount -t tmpfs -o size=2100m tmpfs /mnt/small` for the 1.6 GiB store.

`append-storm` is an ordinary scenario that looks for a fault: sixteen clients append one small
message at a time over kept-alive connections for `--seconds`, and check every byte of every
response (the status line, the length, a message id and nothing after it).

Every scenario prints `"ok"`: whether the node met what the scenario checks. Options:
`--repeat` (reads per case), `--max-messages` (where a read of a large channel stops),
`--seconds` (how long a load phase lasts) and `--metrics-url`.
