//! Logging support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const constants = @import("constants.zig");
const builtin = @import("builtin");

const hissylogz = @import("hissylogz");

pub const Logger = hissylogz.Logger;
pub const LogLevel = hissylogz.LogLevel;
pub const LogFormat = hissylogz.LogFormat;

const auto_logging_filter_level = if (builtin.is_test) LogLevel.fine else LogLevel.info;

pub var default_logging_filter_level = auto_logging_filter_level;
pub var default_logging_format = LogFormat.text;

var logging_mtx: std.Thread.Mutex = .{};

var logger_pool: ?hissylogz.LoggerPool = null;

/// Initializes common logger pool
pub fn init(allocator: std.mem.Allocator, writer: *std.fs.File.Writer) !void {
    logging_mtx.lock();
    defer logging_mtx.unlock();

    if (logger_pool) |_| return;

    logger_pool = try hissylogz.loggerPool(allocator, .{
        .writer = &writer.interface,
        .filter_level = default_logging_filter_level,
        .log_format = default_logging_format,
    });
}

/// De-initializes common logger pool
pub fn deinit() void {
    logging_mtx.lock();
    defer logging_mtx.unlock();

    if (logger_pool) |*pool| {
        pool.deinit();
        logger_pool = null;
    }
}

/// Tears off a logger from the common logger pool
pub fn logger(name: []const u8) *Logger {
    logging_mtx.lock();
    defer logging_mtx.unlock();
    if (logger_pool) |*pool| {
        return pool.logger(name);
    }
    @panic("Initialize logging by invoking logging.init() before using loggers.");
}

test "logging smoke test" {
    std.debug.print("test.logging.smoke\n", .{});

    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);
    try init(allocator, &stderr_writer);
    defer deinit();

    logger("test.logging.smoke").info().str("Something", "Simple").log();
}
