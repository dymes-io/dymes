<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# DDT Command Line Usage

## Queries

### Single message
```
--single <ulid>
```

### Range of messages 
```
-S / --range-start-time <start_time>
-E / --range_end-time <end_time>
-s / --range-start-id <start_ulid>
-e / --range_end-id <end_ulid>
```

### Correlation chain
```
-c / --correlation <tail_ulid>
```

## Filters

### Routing masks

```
-n / --narrowing <routing_mask>
-w / --widening <routing_mask>
```

### KV filters

```
-k / --kv <keyval>
```
> Spaces are significant

```shell

dymes_ddt -S yesterday --kv "rat" # .has
dymes_ddt -S yesterday --kv "rat==true" # .eql
dymes_ddt -S yesterday --kv "rat!=true" # .neq

dymes_ddt -S yesterday --kv "wisdom== I am daft" # .eql

```

