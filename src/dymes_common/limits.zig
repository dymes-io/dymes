//! Common limits.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const builtin = @import("builtin");
const std = @import("std");
const maxInt = std.math.maxInt;

pub const max_yaml_config_file_size: usize = 65535; // Surely 64KiB should be enough for anyone?

pub const default_kv_mmap_size: usize = 10 * 1024 * 1024; // 10MiB memory map by default

pub const max_file_block_transfer_size = switch (builtin.os.tag) {
    .linux => 0x7ffff000,
    .macos, .ios, .watchos, .tvos, .visionos => maxInt(i32),
    else => maxInt(std.meta.Int(.unsigned, @typeInfo(usize).int.bits - 1)),
};
