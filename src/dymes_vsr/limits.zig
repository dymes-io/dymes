//! Dymes VSR Limits.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const dymes_msg = @import("dymes_msg");

/// Maximum size of a Dymes message frame to append.
pub const max_append_frame_size = dymes_msg.limits.max_frame_size; // 2MiB

/// Maximum number of "active" op-log entries.
///
/// The op-log contains the smallest subset of append operations that encompass
/// all possible outstanding ops (hot window), up to `max_oplog_entries`,
/// after which ingestion will stall.
pub const max_oplog_entries: usize = 4000; // Bad news for many large messages
