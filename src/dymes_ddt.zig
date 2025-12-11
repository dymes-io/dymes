//! Dymes Dump Tool.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const common = @import("dymes_common");
const dymes_engine = @import("dymes_engine");
const dymes_msg = @import("dymes_msg");
const clap = @import("clap");
const builtin = @import("builtin");
const zeit = @import("zeit");

const Engine = dymes_engine.Engine;
const EngineMetrics = dymes_engine.EngineMetrics;
const FrameAllocator = dymes_engine.FrameAllocator;
const Allocator = std.mem.Allocator;
const config = common.config;
const Config = common.config.Config;
const ConfigBuilder = common.config.ConfigBuilder;
const Health = common.health.Health;
const LifeCycleControl = common.util.LifeCycleControl;
const Ulid = common.ulid.Ulid;
const Filter = dymes_msg.Filter;
const KvFilter = dymes_msg.KvFilter;
const KvPair = dymes_msg.KvPair;
const Query = dymes_msg.Query;
const errors = common.errors;
const UsageError = errors.UsageError;
const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const ConfigError = UsageError || CreationError || AccessError;
const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const QueryRequest = dymes_engine.QueryRequest;
const Message = dymes_engine.Message;
const rfc3339 = common.rfc3339;

const min_ulid: Ulid = Ulid{
    .time = 0,
    .rand = 0,
};

const max_ulid: Ulid = Ulid{
    .time = 0xFFFFFFFFFFFF,
    .rand = 0xFFFFFFFFFFFF_FFFF_FFFF,
};

const RenderFormat = enum { json, csv };

var process_lifecycle: LifeCycleControl = .{};

const ParsedState = struct {
    config: Config,
    query: Query,
    filters: []Filter,
    render_format: RenderFormat,
    gen_data: bool,

    pub fn init(gpa: std.mem.Allocator, dymes_config: Config, query: Query, filters: []Filter, render_format: RenderFormat, gen_data: bool) CreationError!ParsedState {
        const owned_filters = gpa.dupe(Filter, filters) catch return CreationError.OutOfMemory;
        errdefer gpa.free(owned_filters);

        var engine_config_builder = try config.builder("ddt", gpa);
        try engine_config_builder.merge(dymes_config);
        const final_config = try engine_config_builder.build();
        errdefer final_config.deinit();

        return .{
            .config = final_config,
            .query = query,
            .filters = owned_filters,
            .render_format = render_format,
            .gen_data = gen_data,
        };
    }

    pub fn deinit(self: *ParsedState, allocator: std.mem.Allocator) void {
        defer self.config.deinit();
        defer allocator.free(self.filters);
        for (self.filters) |filter| {
            switch (filter) {
                .kv_filter => |kv| {
                    switch (kv) {
                        .has => |h| {
                            allocator.free(h);
                        },
                        .eql => |pair| {
                            allocator.free(pair.key);
                            allocator.free(pair.value);
                        },
                        .neq => |pair| {
                            allocator.free(pair.key);
                            allocator.free(pair.value);
                        },
                    }
                },
                else => {},
            }
        }

        switch (self.query) {
            .query_multiple => |ulids| {
                allocator.free(ulids);
            },
            else => {},
        }
    }
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var ddt_config = try parseCmdLine(allocator);
    defer ddt_config.deinit(allocator);

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("ddt");

    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    logger.debug().msg("Dymes Data Dump Tool initializing").log();

    var health = Health.init(allocator);
    defer health.deinit();

    var engine_metrics: Engine.EngineMetrics = .init();

    const engine_config = try ddt_config.config.asConfig("engine") orelse return UsageError.OtherUsageFailure;
    var engine = try Engine.init(allocator, frame_allocator, &health, &engine_metrics, engine_config);
    defer engine.deinit();

    var sa: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0x0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    if (ddt_config.gen_data) {
        _ = try generateTestData(logger, allocator, engine);
    }

    logger.info().msg("Dymes Data Dump Tool initialized").log();
    try runDataDumpTool(logger, allocator, engine, ddt_config);
    logger.info().msg("Dymes Data Dump Tool terminating").log();
}

fn parseCmdLine(allocator: Allocator) !ParsedState {
    const params = comptime clap.parseParamsComptime(
        \\-h, --help               Display this help and exit.
        \\--config <str>           Configuration file path.
        \\-d, --data <str>         Data directory path.
        \\-l, --loglevel <str>     Logging filter level. One of ("fine", "debug", "info", "warn", "err", "fatal")
        \\-r, --logformat <str>    Logging format. One of ("json", "text")
        \\
        \\--single <str>           Perform a query for a single entry. Value passed must be a single ULID
        \\-c, --correlation <str>  Perform a query using a correlation ID. Value passed must be a single ULID
        \\
        \\-s, --start <str>        Perform a range query using a specified start time. Start value may be a ULID, a variety of RFC3339 timestamps, or keywords like "now" or "sod" with optional offset (e.g +1h30m)
        \\-e, --end <str>          (Optional) Specifies the end range of the range query. Accepts same values as --start. If no end is specified then it assumes
        \\
        \\-n, --narrowing <str>...  Apply a narrowing routing filter (bitmask)
        \\-w, --widening <str>...   Apply a widening routing filter (bitmask)
        \\
        \\-k, --kv <str>...         Apply a key/value filter, e.g. "priority==high". Support has, equal, and not equals. Multiple kv filters can be supplied
        \\
        \\-r, --render <str>       Specifiy rendering format of data. One of ("json", "csv"). Default is json.
        \\
        \\--gentestdata            Generate test data before running queries. Used for testing and debugging.
        \\
        \\@mode(single, correlation, range)
    );

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        {
            var stderr_buffer: [512]u8 = undefined;
            var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
            const stderr = &stderr_writer.interface;
            diag.report(stderr, err) catch {};
            stderr.flush() catch {};
            return err;
        }
    };
    defer res.deinit();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (res.args.help != 0 or args.len == 1) {
        var stderr_buffer: [512]u8 = undefined;
        var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
        const stderr = &stderr_writer.interface;
        try clap.help(stderr, clap.Help, &params, .{});
        stderr.flush() catch {};
        std.posix.exit(0);
    }

    if ((res.args.single != null and res.args.correlation != null) or
        (res.args.single != null and res.args.start != null) or
        (res.args.correlation != null and res.args.start != null))
    {
        std.debug.print("Error: --single, --correlation, and --start are mutually exclusive\n", .{});
        return error.InvalidArguments;
    }

    if (res.args.end != null and res.args.start == null) {
        std.debug.print("Error: --end cannot be used without --start\n", .{});
        return error.InvalidArguments;
    }

    const query: Query = qry_val: {
        if (res.args.single) |single_str| {
            const single_id = common.ulid.decode(single_str) catch return error.InvalidUlid;
            break :qry_val Query.single(single_id);
        } else if (res.args.start) |start_str| {
            const start_id = blk: {
                const instant: zeit.Instant = try rfc3339.parseInstant(start_str);
                const ms = @as(i64, @intCast(@divTrunc(instant.timestamp, std.time.ns_per_ms)));
                break :blk Ulid{
                    .time = @as(u48, @intCast(ms)),
                    .rand = 0,
                };
            };
            if (res.args.end) |end_str| {
                const end_id = blk: {
                    const instant: zeit.Instant = try rfc3339.parseInstant(end_str);
                    const ms = @as(i64, @intCast(@divTrunc(instant.timestamp, std.time.ns_per_ms)));
                    break :blk Ulid{
                        .time = @as(u48, @intCast(ms)),
                        .rand = 0xFFFFFFFFFFFF_FFFF_FFFF,
                    };
                };
                break :qry_val Query.range(start_id, end_id);
            } else {
                break :qry_val Query.range(start_id, max_ulid);
            }
        } else if (res.args.correlation) |corr_str| {
            const corr_id = common.ulid.decode(corr_str) catch return error.InvalidUlid;
            break :qry_val Query.correlation(corr_id);
        } else {
            return error.MissingQuery;
        }
    };

    var filter_list = std.array_list.Managed(Filter).init(allocator);
    defer filter_list.deinit();

    {
        const argv = try std.process.argsAlloc(allocator);
        defer std.process.argsFree(allocator, argv);

        var i: usize = 1;
        while (i < argv.len) : (i += 1) {
            const arg = argv[i];

            if (std.mem.eql(u8, arg, "-n")) {
                if (i + 1 >= argv.len) return error.InvalidArguments;

                const next = argv[i + 1];
                if (next.len > 0 and next[0] == '-') return error.InvalidArguments;

                const mask = try std.fmt.parseInt(u128, next, 0);
                try filter_list.append(Filter.routingFilter(.{
                    .routing_mask = mask,
                    .routing_cmp = mask,
                    .variant = .narrowing,
                }));
                i += 1;
            } else if (std.mem.eql(u8, arg, "-w")) {
                if (i + 1 >= argv.len) return error.InvalidArguments;

                const next = argv[i + 1];
                if (next.len > 0 and next[0] == '-') return error.InvalidArguments;

                const mask = try std.fmt.parseInt(u128, next, 0);
                try filter_list.append(Filter.routingFilter(.{
                    .routing_mask = mask,
                    .variant = .widening,
                }));
                i += 1;
            } else if (std.mem.eql(u8, arg, "-k")) {
                if (i + 1 >= argv.len) return error.InvalidArguments;

                const next = argv[i + 1];
                if (next.len > 0 and next[0] == '-') return error.InvalidArguments;

                const kv_filter = try parseKvExpr(next, allocator);
                try filter_list.append(try Filter.kvFilter(kv_filter));
                i += 1;
            } else if (std.mem.startsWith(u8, arg, "--kv")) {
                const kv_filter = try parseKvExpr(arg[5..], allocator);
                try filter_list.append(try Filter.kvFilter(kv_filter));
            } else if (std.mem.startsWith(u8, arg, "--widening")) {
                const mask = try std.fmt.parseInt(u128, arg[5..], 0);
                try filter_list.append(Filter.routingFilter(.{
                    .routing_mask = mask,
                    .variant = .widening,
                }));
            } else if (std.mem.startsWith(u8, arg, "--narrowing")) {
                const mask = try std.fmt.parseInt(u128, arg[5..], 0);
                try filter_list.append(Filter.routingFilter(.{
                    .routing_mask = mask,
                    .variant = .widening,
                }));
            }
        }
    }

    var render_format: RenderFormat = .json;
    if (res.args.render) |_render_format| {
        if (std.mem.eql(u8, _render_format, "csv")) {
            render_format = .csv;
        } else if (std.mem.eql(u8, _render_format, "json")) {
            render_format = .json;
        } else {
            std.debug.print("Error: invalid --render format: {s}\n", .{_render_format});
            return error.InvalidArguments;
        }
    }

    const config_path = res.args.config orelse "conf/dymes-ddt.yaml";
    var dymes_config = try buildBaseConfig(allocator, config_path);
    defer dymes_config.deinit();

    if (res.args.data) |data_dir| {
        var cfg_bld = try ConfigBuilder.init("ddt", allocator);
        defer cfg_bld.deinit();

        try cfg_bld.merge(dymes_config);
        try cfg_bld.withString("engine.store.data_path", data_dir);

        const new_config = try cfg_bld.build();
        dymes_config.deinit();
        dymes_config = new_config;
    }

    var desired_logging_level: LogLevel = .info;
    if (try dymes_config.asString("ddt.logging.level")) |logging_level_name| {
        desired_logging_level = try parseLoggingLevelName(logging_level_name);
    }
    if (res.args.loglevel) |override_level| {
        desired_logging_level = try parseLoggingLevelName(override_level);
    }
    logging.default_logging_filter_level = desired_logging_level;

    var desired_log_format: LogFormat = .text;
    if (try dymes_config.asString("ddt.logging.format")) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    if (res.args.logformat) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    logging.default_logging_format = desired_log_format;

    return .init(
        allocator,
        dymes_config,
        query,
        filter_list.items,
        render_format,
        res.args.gentestdata != 0,
    );
}

fn runDataDumpTool(logger: *logging.Logger, allocator: Allocator, engine: *Engine, parsedState: ParsedState) !void {
    logger.debug().msg("Running data dump").log();
    const query = parsedState.query;

    var query_obj: Query = undefined;

    switch (query) {
        .query_single => |q| {
            query_obj = Query.single(q);
        },
        .query_correlation => |q| {
            query_obj = Query.correlation(q);
        },
        .query_range => |q| {
            query_obj = Query.range(q.range_start, q.range_end);
        },
        else => {},
    }
    var qrb = try QueryRequest.Builder.init(allocator);
    defer qrb.deinit();

    qrb.withQuery(query_obj);
    for (parsedState.filters) |filter| {
        try qrb.withFilter(filter);
    }
    const query_request = try qrb.build();
    defer query_request.deinit();
    var cursor = try engine.query(query_request);
    defer cursor.close();

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    if (parsedState.render_format == .csv) {
        try stdout.print("timestamp, id, correlation_id, channel, routing, headers, content\n", .{});
    }

    while (try cursor.next()) |_message| {
        switch (parsedState.render_format) {
            .csv => try _message.renderAsCSV(stdout),
            .json => try _message.renderAsJSON(stdout),
        }
    }
}

fn buildBaseConfig(allocator: std.mem.Allocator, config_path: []const u8) ConfigError!Config {
    var config_bld = try config.builder("dymes_ddt", allocator);
    defer config_bld.deinit();
    {
        var yaml_cfg: ?Config = try buildYamlConfig(allocator, config_path);
        if (yaml_cfg) |*yaml| {
            defer yaml.deinit();
            try config_bld.merge(yaml.*);
        }

        var env_cfg = try config.fromEnvironment(allocator);
        defer env_cfg.deinit();

        try config_bld.merge(env_cfg);
    }
    return try config_bld.build();
}

fn buildYamlConfig(allocator: std.mem.Allocator, config_path: []const u8) ConfigError!?Config {
    var yaml_cfg = config.fromYamlPath(std.fs.cwd(), config_path, allocator) catch |e| {
        if (e == config.Error.FileNotFound) {
            return null;
        }
        return e;
    };
    _ = &yaml_cfg;
    return yaml_cfg;
}

fn parseLoggingLevelName(logging_level_name: []const u8) UsageError!LogLevel {
    if (std.mem.eql(u8, @tagName(LogLevel.fine), logging_level_name)) {
        return .fine;
    } else if (std.mem.eql(u8, @tagName(LogLevel.debug), logging_level_name)) {
        return .debug;
    } else if (std.mem.eql(u8, @tagName(LogLevel.info), logging_level_name)) {
        return .info;
    } else if (std.mem.eql(u8, @tagName(LogLevel.warn), logging_level_name)) {
        return .warn;
    } else if (std.mem.eql(u8, @tagName(LogLevel.err), logging_level_name)) {
        return .err;
    } else if (std.mem.eql(u8, @tagName(LogLevel.fatal), logging_level_name)) {
        return .fatal;
    }

    return UsageError.IllegalArgument;
}

fn parseKvExpr(expr: []const u8, allocator: Allocator) !KvFilter {
    if (std.mem.indexOf(u8, expr, "==")) |i| {
        return KvFilter.equals(KvPair.kv(try allocator.dupe(u8, expr[0..i]), try allocator.dupe(u8, expr[i + 2 ..])));
    }
    if (std.mem.indexOf(u8, expr, "!=")) |i| {
        return KvFilter.notEquals(KvPair.kv(try allocator.dupe(u8, expr[0..i]), try allocator.dupe(u8, expr[i + 2 ..])));
    }
    return KvFilter.having(try allocator.dupe(u8, expr));
}

fn signalHandler(sig: i32) callconv(.c) void {
    switch (sig) {
        std.posix.SIG.INT, std.posix.SIG.TERM => process_lifecycle.shutdown(),
        else => {},
    }
}

fn parseLoggingFormatName(logging_format_name: []const u8) UsageError!LogFormat {
    if (std.mem.eql(u8, "json", logging_format_name)) {
        return .json;
    } else if (std.mem.eql(u8, "text", logging_format_name)) {
        return .text;
    }
    return UsageError.IllegalArgument;
}

fn parseRfc3339Ulid(str: []const u8, is_end: bool) !Ulid {
    if (std.mem.eql(u8, str, "now")) {
        const ts: i64 = std.time.milliTimestamp();
        return Ulid{
            .time = @as(u48, @intCast(ts)),
            .rand = if (is_end) 0xFFFFFFFFFFFF_FFFF_FFFF else 0,
        };
    } else if (std.mem.eql(u8, str, "sod")) {
        const ts: i64 = std.time.milliTimestamp();
        const millis: i64 = ts - @rem(ts, std.time.ms_per_day);
        return Ulid{
            .time = @as(u48, @intCast(millis)),
            .rand = if (is_end) 0xFFFFFFFFFFFF_FFFF_FFFF else 0,
        };
    }

    if (str.len < 19) return error.InvalidTimestamp;

    const year = try std.fmt.parseInt(u64, str[0..4], 10);
    const month = try std.fmt.parseInt(u64, str[5..7], 10);
    const day = try std.fmt.parseInt(u64, str[8..10], 10);
    const hour = try std.fmt.parseInt(u64, str[11..13], 10);
    const min = try std.fmt.parseInt(u64, str[14..16], 10);
    const sec = try std.fmt.parseInt(u64, str[17..19], 10);

    var days: u64 = 0;
    var y: u64 = 1970;
    while (y < year) : (y += 1) {
        days += if ((y % 4 == 0 and (y % 100 != 0 or y % 400 == 0))) 366 else 365;
    }

    const month_days = [_]u64{ 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var m: u64 = 1;
    while (m < month) : (m += 1) {
        days += month_days[m];
        if (m == 2 and (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))) {
            days += 1;
        }
    }

    days += day - 1;

    const millis: u64 =
        days * std.time.ms_per_day + hour * std.time.ms_per_hour + min * std.time.ms_per_min + sec * std.time.ms_per_s;

    return Ulid{
        .time = @as(u48, @intCast(millis)),
        .rand = if (is_end) 0xFFFFFFFFFFFF_FFFF_FFFF else 0,
    };
}

const FirstAndLast = struct {
    first: Ulid,
    last: Ulid,
};

fn generateTestData(logger: *logging.Logger, allocator: std.mem.Allocator, engine: *Engine) !FirstAndLast {
    logger.info().msg("Generating test data...").log();

    var firstAndLast: FirstAndLast = undefined;

    const colours: [5][]const u8 = .{ "black", "blue", "purple", "red", "green" };
    const priorities: [3][]const u8 = .{ "high", "medium", "low" };
    const flavours: [4][]const u8 = .{ "mild", "medium", "hot", "reaper" };

    const channels: [3]u128 = .{ 1, 2, 3 };
    const routings: [4]u128 = .{ 0, 100, 200, 300 };

    var kv = std.StringArrayHashMap([]const u8).init(allocator);
    defer kv.deinit();
    try kv.put("colour", "");
    try kv.put("priority", "");
    try kv.put("flavour", "");

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        kv.put("colour", colours[i % colours.len]) catch unreachable;
        kv.put("priority", priorities[i % priorities.len]) catch unreachable;
        kv.put("flavour", flavours[i % flavours.len]) catch unreachable;

        const body = try std.fmt.allocPrint(allocator, "Test message {d}", .{i});
        defer allocator.free(body);

        const channel = channels[i % channels.len];
        const routing = routings[i % routings.len];

        if (i == 0) {
            firstAndLast.first = try engine.append(channel, routing, body, .{ .transient_kv_headers = kv });
        } else if (i == 99) {
            firstAndLast.last = try engine.append(channel, routing, body, .{ .transient_kv_headers = kv });
        } else {
            _ = try engine.append(channel, routing, body, .{ .transient_kv_headers = kv });
        }
    }
    logger.info()
        .msg("Test data generated")
        .any("range_start", firstAndLast.first)
        .any("range_end", firstAndLast.last)
        .log();
    return firstAndLast;
}

test "generateTestData inserts 100 messages" {
    const allocator = std.testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const logger = logging.logger("test.generateTestData");

    var dummy_filters: [0]Filter = .{};
    const dummy_query = Query.range(min_ulid, max_ulid);

    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var health = Health.init(allocator);
    defer health.deinit();

    logger.fine().msg("Preparing test config").log();
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("ddt-sanity-check");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "ddt-sanity-check");
    defer allocator.free(abs_dir_name);

    var cfg_bld = try ConfigBuilder.init("test", allocator);
    defer cfg_bld.deinit();

    try cfg_bld.withInt("engine.store.id", 1234);
    try cfg_bld.withInt("engine.datasets", 1);
    try cfg_bld.withString("engine.store.data_path", abs_dir_name);

    var test_dymes_config = try cfg_bld.build();

    logger.debug()
        .msg("Test config prepared")
        .str("data_path", try test_dymes_config.asString("engine.store.data_path"))
        .log();

    var ddt_test_config = ParsedState{
        .config = test_dymes_config,
        .query = dummy_query,
        .filters = dummy_filters[0..],
        .render_format = .json,
        .gen_data = true,
    };
    defer ddt_test_config.deinit(allocator);

    var engine_metrics: EngineMetrics = .init();

    const engine_config = try ddt_test_config.config.asConfig("engine") orelse return UsageError.OtherUsageFailure;
    var engine = try Engine.init(allocator, frame_allocator, &health, &engine_metrics, engine_config);
    defer engine.deinit();

    _ = try generateTestData(logger, allocator, engine);

    var qrb = try QueryRequest.Builder.init(allocator);
    defer qrb.deinit();

    qrb.withQuery(ddt_test_config.query);
    const qr = try qrb.build();
    defer qr.deinit();

    var cursor = try engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| count += 1;

    try std.testing.expectEqual(@as(usize, 100), count);
}

const TestCtx = struct {
    logger: *logging.Logger,
    allocator: Allocator,
    engine: *Engine,
    filters: []const Filter,
    query: Query,
};

fn wrapper(realTest: *const fn (ctx: TestCtx) anyerror!void, filters: []const []const u8, query: Query) !void {
    const allocator = std.testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const logger = logging.logger("test.ddt");
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();
    var health = Health.init(allocator);
    defer health.deinit();
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    try tmp_dir.dir.makeDir("ddt-sanity-check");
    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "ddt-sanity-check");
    defer allocator.free(abs_dir_name);
    var cfg_bld = try ConfigBuilder.init("test", allocator);
    defer cfg_bld.deinit();
    try cfg_bld.withInt("engine.store.id", 1234);
    try cfg_bld.withInt("engine.datasets", 1);
    try cfg_bld.withString("engine.store.data_path", abs_dir_name);
    var test_dymes_config = try cfg_bld.build();
    defer test_dymes_config.deinit();
    const engine_config = try test_dymes_config.asConfig("engine") orelse return UsageError.OtherUsageFailure;
    var engine_metrics: Engine.EngineMetrics = .init();

    var engine = try Engine.init(allocator, frame_allocator, &health, &engine_metrics, engine_config);
    defer engine.deinit();
    var parsedFilters = std.array_list.Managed(Filter).init(allocator);
    errdefer {
        for (parsedFilters.items) |filter| {
            switch (filter) {
                .kv_filter => |kv| {
                    switch (kv) {
                        .has => |h| allocator.free(h),
                        .eql => |pair| {
                            allocator.free(pair.key);
                            allocator.free(pair.value);
                        },
                        .neq => |pair| {
                            allocator.free(pair.key);
                            allocator.free(pair.value);
                        },
                    }
                },
                else => {},
            }
        }
        parsedFilters.deinit();
    }
    for (filters) |f| {
        const parsedKv = try parseKvExpr(f, allocator);
        try parsedFilters.append(try Filter.kvFilter(parsedKv));
    }

    try realTest(.{
        .logger = logger,
        .allocator = allocator,
        .engine = engine,
        .filters = parsedFilters.items,
        .query = query,
    });

    for (parsedFilters.items) |filter| {
        switch (filter) {
            .kv_filter => |kv| {
                switch (kv) {
                    .has => |h| allocator.free(h),
                    .eql => |pair| {
                        allocator.free(pair.key);
                        allocator.free(pair.value);
                    },
                    .neq => |pair| {
                        allocator.free(pair.key);
                        allocator.free(pair.value);
                    },
                }
            },
            else => {},
        }
    }
    parsedFilters.deinit();
}

test "kvfilterEQL" {
    const filters = &[_][]const u8{"flavour==reaper"};
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&kvfilterEQLRealTest, filters, query);
}

fn kvfilterEQLRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_message| {
        try std.testing.expect(_message.containsKeyValue("flavour", "reaper"));
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 25), count);
}

test "kvfilterNEQ" {
    const filters = &[_][]const u8{"flavour!=mild"};
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&kvfilterNEQRealTest, filters, query);
}

fn kvfilterNEQRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_message| {
        try std.testing.expect(!_message.containsKeyValue("flavour", "mild"));
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 75), count);
}

test "kvfilterHAS" {
    const filters = &[_][]const u8{"flavour"};
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&kvfilterHASRealTest, filters, query);
}

fn kvfilterHASRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "kvfilterEQLNoMatch" {
    const filters = &[_][]const u8{"priority==super high"};
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&kvfilterEQLNoMatchRealTest, filters, query);
}

fn kvfilterEQLNoMatchRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "kvfilterNEQNoMatch" {
    const filters = &[_][]const u8{ "priority!=low", "priority!=medium", "priority!=high" };
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&kvfilterNEQNoMatchRealTest, filters, query);
}

fn kvfilterNEQNoMatchRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "kvfilterHASNoMatch" {
    const filters = &[_][]const u8{"planet"};
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&kvfilterHASNoMatchRealTest, filters, query);
}

fn kvfilterHASNoMatchRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "singlequery" {
    const filters = &[_][]const u8{};
    const dummy_query = Query.range(min_ulid, max_ulid);
    try wrapper(&singlequeryRealTest, filters, dummy_query);
}

fn singlequeryRealTest(ctx: TestCtx) !void {
    const firstAndLast = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);
    const realQuery = Query.single(firstAndLast.first);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(realQuery);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "singlequeryinvalid" {
    const filters = &[_][]const u8{};
    const dummy_query = Query.range(min_ulid, max_ulid);
    try wrapper(&singlequeryinvalidRealTest, filters, dummy_query);
}

fn singlequeryinvalidRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);
    const realQuery = Query.single(max_ulid);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(realQuery);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "rangequery" {
    const filters = &[_][]const u8{};
    const dummy_query = Query.range(min_ulid, max_ulid);
    try wrapper(&rangequeryRealTest, filters, dummy_query);
}

fn rangequeryRealTest(ctx: TestCtx) !void {
    const firstAndLast = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);
    const realQuery = Query.range(firstAndLast.first, firstAndLast.last);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(realQuery);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "rangequeryinvalid" {
    const filters = &[_][]const u8{};
    const dummy_query = Query.range(min_ulid, max_ulid);
    try wrapper(&rangequeryinvalidRealTest, filters, dummy_query);
}

fn rangequeryinvalidRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);
    const realQuery = Query.range(min_ulid, Ulid{
        .time = 1,
        .rand = 0,
    });

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(realQuery);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "multiplefilters" {
    const filters = &[_][]const u8{ "flavour==hot", "priority==high" };
    const query = Query.range(min_ulid, max_ulid);
    try wrapper(&multiplefiltersRealTest, filters, query);
}

fn multiplefiltersRealTest(ctx: TestCtx) !void {
    _ = try generateTestData(ctx.logger, ctx.allocator, ctx.engine);

    var qrb = try QueryRequest.Builder.init(ctx.allocator);
    defer qrb.deinit();
    qrb.withQuery(ctx.query);
    for (ctx.filters) |_filter| {
        try qrb.withFilter(_filter);
    }
    const qr = try qrb.build();
    defer qr.deinit();
    var cursor = try ctx.engine.query(qr);
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    ctx.logger.debug()
        .msg("Verified test message matches")
        .int("count", count)
        .log();
    try std.testing.expectEqual(@as(usize, 8), count);
}
