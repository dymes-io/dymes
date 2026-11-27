//! Dymes benchmark harness entrypoint.
//!
//! `dymes_bench gen` writes a reproducible store of a chosen size; `dymes_bench run` (see
//! `doc/benchmarks.md`) drives a running node with it.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const common = @import("dymes_common");
const logging = common.logging;

const gen = @import("dymes_bench/gen.zig");
const scenarios = @import("dymes_bench/scenarios.zig");

const usage =
    \\Usage:
    \\  dymes_bench gen --dir <store dir> --manifest <file> [--messages <n>] [--channels <n>]
    \\                  [--seed <n>] [--days <n>] [--end-ms <ms since epoch>]
    \\  dymes_bench run <scenario> --url <http://host:port> --manifest <file>
    \\                  [--repeat <n>] [--max-messages <n>] [--seconds <n>]
    \\                  [--metrics-url <url>]
    \\
    \\Scenarios: channel-read, tracking-lookup, sparse-routing, newest, cursor-leak,
    \\           abandoned-traversal, mixed-load, rollover
    \\
;

pub const std_options = std.Options{ .log_level = .warn };

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const log_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(log_buffer);
    var stderr_writer = std.fs.File.stderr().writer(log_buffer);
    logging.default_logging_filter_level = logging.LogLevel.warn;
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    if (args.len < 2) return fail("a command is required");
    const command = args[1];
    const rest = args[2..];

    if (std.mem.eql(u8, command, "gen")) {
        var options: gen.Options = .{ .dir = "", .manifest = "" };
        var i: usize = 0;
        while (i < rest.len) : (i += 2) {
            const name = rest[i];
            if (i + 1 >= rest.len) return fail("an option is missing its value");
            const value = rest[i + 1];
            if (std.mem.eql(u8, name, "--dir")) {
                options.dir = value;
            } else if (std.mem.eql(u8, name, "--manifest")) {
                options.manifest = value;
            } else if (std.mem.eql(u8, name, "--messages")) {
                options.messages = try std.fmt.parseUnsigned(u64, value, 10);
            } else if (std.mem.eql(u8, name, "--channels")) {
                options.channels = try std.fmt.parseUnsigned(usize, value, 10);
            } else if (std.mem.eql(u8, name, "--seed")) {
                options.seed = try std.fmt.parseUnsigned(u64, value, 10);
            } else if (std.mem.eql(u8, name, "--days")) {
                options.days = try std.fmt.parseUnsigned(u64, value, 10);
            } else if (std.mem.eql(u8, name, "--end-ms")) {
                options.end_ms = try std.fmt.parseUnsigned(u64, value, 10);
            } else return fail("unknown option");
        }
        if (options.dir.len == 0 or options.manifest.len == 0) return fail("--dir and --manifest are required");
        try gen.run(allocator, options);
    } else if (std.mem.eql(u8, command, "run")) {
        if (rest.len < 1) return fail("a scenario is required");
        var options: scenarios.Options = .{ .scenario = rest[0], .url = "", .manifest = "" };
        var i: usize = 1;
        while (i < rest.len) : (i += 2) {
            const name = rest[i];
            if (i + 1 >= rest.len) return fail("an option is missing its value");
            const value = rest[i + 1];
            if (std.mem.eql(u8, name, "--url")) {
                options.url = value;
            } else if (std.mem.eql(u8, name, "--manifest")) {
                options.manifest = value;
            } else if (std.mem.eql(u8, name, "--repeat")) {
                options.repeat = try std.fmt.parseUnsigned(usize, value, 10);
            } else if (std.mem.eql(u8, name, "--max-messages")) {
                options.max_messages = try std.fmt.parseUnsigned(u64, value, 10);
            } else if (std.mem.eql(u8, name, "--seconds")) {
                options.seconds = try std.fmt.parseUnsigned(u64, value, 10);
            } else if (std.mem.eql(u8, name, "--metrics-url")) {
                options.metrics_url = value;
            } else return fail("unknown option");
        }
        if (options.url.len == 0 or options.manifest.len == 0) return fail("--url and --manifest are required");
        try scenarios.run(allocator, options);
    } else return fail("unknown command");
}

fn fail(reason: []const u8) error{Usage} {
    std.debug.print("dymes_bench: {s}\n{s}", .{ reason, usage });
    return error.Usage;
}
