<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Dymes Component Flow

These diagrams show the [Dymes components](dymes-components.md) involved during high-level operations. 
It may be safely assumed that `dymes_common` and `dymes_msg` would be involved throughout.  

## Appending a message

Components involved when appending a message.

```mermaid
flowchart TD

    CreationRequest>"CreationRequest"] ---> dymes_http_server

    VsrRequest>"VsrRequest"] --> dymes_vsr_transport
    dymes_http_shared_ctx --> Ingester

    subgraph dymes_http["HTTP"]
        dymes_http_server[["Server"]] --> dymes_http_req_ctx 
        dymes_http_req_ctx --> dymes_http_shared_ctx["Shared Context"]
    end

    subgraph dymes_vsr["VSR"]
        dymes_vsr_transport[["VSR Transport"]] --> dymes_vsr_machine 
        dymes_vsr_machine[["VSR State Machine"]] --> Ingester
    end

    Ingester --> StorageSubsystem
    
    subgraph dymes_engine["Engine"]

        StorageSubsystem --> AppendStore("AppendStore")

        AppendStore --> DataSet
        
        DataSet --> SegmentsCache

        SegmentsCache --> DataSegment
        
        DataSegment --> MessageFile --> fs_msg_file[("Filesystem")]
        
        DataSegment --> IndexFile --> fs_idx_file[("Filesystem")]
        
        DataSet --> JournalFile --> fs_jnl_file[("Filesystem")]

        StorageSubsystem -.-> ImmutableStore("ImmutableStore")

        ImmutableStore --> SegmentsUlidIndex

        ImmutableStore --> ReadDataSet

        ReadDataSet --> MessageIndex
        
        ReadDataSet --> ChannelIndex

    end
    
```



## Querying messages

Components involved when querying messages.

```mermaid
flowchart TD

    QueryDTO>"Query"] ---> dymes_http_server

    subgraph dymes_http["HTTP"]
        dymes_http_server[["Server"]] --> dymes_http_req_ctx["Request Context"]
        dymes_http_req_ctx --> dymes_http_shared_ctx["Shared Context"] 
    end

    subgraph dymes_engine["Engine"]

        dymes_http_shared_ctx --> QuerySubsystem

        QuerySubsystem --> ImmutableStore("ImmutableStore")
        
        ImmutableStore --> SegmentsUlidIndex

        ImmutableStore --> ReadDataSet

        ReadDataSet --> SegmentsCache

        SegmentsCache --> DataSegment
        
        ReadDataSet --> MessageIndex
        
        ReadDataSet --> ChannelIndex

        DataSegment --> MessageFile --> fs_msg_file[("Filesystem")]

        DataSegment --> IndexFile --> fs_idx_file[("Filesystem")]

    end
    
```



