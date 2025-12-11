//! Dymes Message Store Constants.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const limits = @import("limits.zig");
const assert = @import("std").debug.assert;

pub const message_file_header_magic: [16]u8 = .{ 'D', 'I', 'M', 'E', 'S', 'd', 'i', 'm', 'e', 's', 'M', 'S', 'G', 'v', 0, 1 };
pub const index_file_header_magic: [16]u8 = .{ 'D', 'I', 'M', 'E', 'S', 'd', 'i', 'm', 'e', 's', 'I', 'D', 'X', 'v', 0, 1 };

pub const msg_file_name_ext: []const u8 = "msg";
pub const msg_file_name_fmt: []const u8 = "dymes_{d}.msg";

pub const idx_file_name_ext: []const u8 = "idx";
pub const idx_file_name_fmt: []const u8 = "dymes_{d}.idx";

pub const hot_journal_file_name: []const u8 = "dymes.journal";

/// Level of filesystem synchronization.
pub const SyncLevel = enum {
    /// No filesystem synchronization. Not advised.
    none,
    /// Synchronize only the journal file.
    journal_only,
    /// Synchronize both the journal and message files.
    msg_and_journal,
    /// Synchronize the journal, message, and index files.
    all,
};

/// Default filesystem synchronization level.
pub const default_sync_level: SyncLevel = .msg_and_journal;
