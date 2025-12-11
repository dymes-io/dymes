//! Dymes Metrics Exporter.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const httpz = @import("httpz");
const clap = @import("clap");

const dymes_common = @import("dymes_common");

const logging = dymes_common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const Logger = logging.Logger;

const metrics = dymes_common.metrics;
const ExporterExitCode = metrics.ExporterExitCode;

const EngineMetrics = @import("dymes_engine").EngineMetrics;
const HttpMetrics = @import("dymes_http").HttpMetrics;

const DymesMetrics = @import("dymes_metrics.zig").DymesMetrics(EngineMetrics, HttpMetrics);

fn die(code: ExporterExitCode) noreturn {
    std.process.exit(@intFromEnum(code));
}

/// Struct that contains the passed arguments to the process
const Args = struct {
    port: u16,
    path: []u8,
};

/// Function that parses the passed arguments and stores them in the Args struct
fn parseArgs(allocator: std.mem.Allocator) Args {
    const params = comptime clap.parseParamsComptime(
        \\-f, --file <str>         Path to the mmap file. (REQUIRED)
        \\-p, --port <u16>         Port for the metrics endpoint. (REQUIRED)
    );

    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{ .allocator = allocator }) catch
        die(ExporterExitCode.usage);
    defer res.deinit();

    const port: u16 = res.args.port orelse die(ExporterExitCode.usage);
    if (port == 0) die(ExporterExitCode.usage);

    const path_in = res.args.file orelse die(ExporterExitCode.usage);
    const path = allocator.dupe(u8, path_in) catch die(ExporterExitCode.usage);

    return .{ .port = port, .path = path };
}

/// Formats the data in the metrics memory-mapped file and lets prometheus pull it
fn metricsHandler(m: *DymesMetrics, req: *httpz.Request, res: *httpz.Response) !void {
    _ = req;
    res.content_type = .TEXT;
    const w = res.writer();

    m.format(w) catch die(ExporterExitCode.write);
    w.flush() catch die(ExporterExitCode.write);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = parseArgs(allocator);
    defer allocator.free(args.path);

    const mapper = metrics.MetricsMapper(DymesMetrics).init(args.path) catch die(ExporterExitCode.mmap);
    defer mapper.deinit();

    const mptr: *DymesMetrics = mapper.metrics;

    var server = httpz.Server(*DymesMetrics).init(allocator, .{ .port = args.port }, mptr) catch
        die(ExporterExitCode.server);
    defer {
        server.stop();
        server.deinit();
    }

    var router = server.router(.{}) catch die(ExporterExitCode.server);
    router.get("/observe/metrics", metricsHandler, .{});

    server.listen() catch die(ExporterExitCode.listen);
    die(ExporterExitCode.ok);
}
