//! Dymes Message Store Options.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const Ulid = common.ulid.Ulid;

const limits = @import("limits.zig");
const constants = @import("constants.zig");

const AllocationError = common.errors.AllocationError;
const AccessError = common.errors.AccessError;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;

const dymes_msg = @import("dymes_msg");

pub const OptionsError = AllocationError || AccessError;

const Self = @This();

/// Data directory path
data_path: []const u8,

/// Data directory
data_dir: std.fs.Dir,

/// Health monitor
health: *Health,

pub fn init(
    allocator: std.mem.Allocator,
    data_dir: std.fs.Dir,
    data_path: []const u8,
    health: *Health,
) OptionsError!Self {
    return .{
        .data_path = allocator.dupe(u8, data_path) catch return OptionsError.OutOfMemory,
        .data_dir = data_dir,
        .health = health,
    };
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    defer allocator.free(self.data_path);
}

const MessageStoreOptions = @This();
test "MessageStore.Options" {
    std.debug.print("test.MessageStore.Options\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var health_monitor = Health.init(allocator);
    defer health_monitor.deinit();

    var options = try MessageStoreOptions.init(
        allocator,
        tmp_dir.dir,
        "dummy",
        &health_monitor,
    );
    defer options.deinit(allocator);
}
