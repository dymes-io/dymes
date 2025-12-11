//! Dymes Data Importer.
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
const dymes_client = @import("dymes_client");
const dymes_http = @import("dymes_http");

const Engine = dymes_engine.Engine;
const EngineMetrics = dymes_engine.EngineMetrics;
const FrameAllocator = dymes_engine.FrameAllocator;
const Allocator = std.mem.Allocator;
const config = common.config;
const Config = common.config.Config;
const ConfigBuilder = common.config.ConfigBuilder;
const Health = common.health.Health;
const HealthProbeListener = common.net.k8s.HealthProbeListener;
const LifeCycleControl = common.util.LifeCycleControl;
const Ulid = common.ulid.Ulid;
const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const QueryRequest = dymes_engine.QueryRequest;
const Message = dymes_engine.Message;
const rfc3339 = common.rfc3339;
const Server = dymes_http.Server;
const Client = dymes_client.Client;
const ClientQueryDto = dymes_client.ClientQueryDto;
const errors = common.errors;
const UsageError = errors.UsageError;
const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const ConfigError = UsageError || CreationError || AccessError;

const Ingester = dymes_http.Ingester;
const StandaloneIngester = dymes_http.StandaloneIngester;
const ClusteredIngester = dymes_http.ClusteredIngester;
const Allocators = dymes_http.Allocators;

const HttpMetrics = dymes_http.HttpMetrics;

const DymesMetrics = @import("dymes_metrics.zig").DymesMetrics(EngineMetrics, HttpMetrics);

var process_lifecycle: LifeCycleControl = .{};

var prng = std.Random.DefaultPrng.init(42);

const ParsedState = struct {
    config: Config,
    reader: *std.Io.Reader,
    file_reader: *std.fs.File.Reader,
    reader_buffer: []u8,
    allocator: std.mem.Allocator,
    start_ulid: Ulid,

    pub fn init(gpa: std.mem.Allocator, dymes_config: Config, reader: *std.Io.Reader, file_reader: *std.fs.File.Reader, reader_buffer: []u8, start_ulid: Ulid) CreationError!ParsedState {
        var engine_config_builder = try config.builder("dymes-ddi", gpa);
        try engine_config_builder.merge(dymes_config);
        const final_config = try engine_config_builder.build();
        errdefer final_config.deinit();

        return .{
            .allocator = gpa,
            .config = final_config,
            .reader = reader,
            .file_reader = file_reader,
            .reader_buffer = reader_buffer,
            .start_ulid = start_ulid,
        };
    }

    pub fn deinit(self: *ParsedState) void {
        self.config.deinit();
        self.allocator.destroy(self.file_reader);
        self.allocator.free(self.reader_buffer);
    }
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var data_importer_config = try parseCmdLine(allocator);
    defer data_importer_config.deinit();

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("ddi");

    logger.debug().msg("Dymes Data Importer Tool initializing").log();

    const client_config = try data_importer_config.config.asConfig("importer.client") orelse return UsageError.OtherUsageFailure;

    var sa: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0x0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    const client_uri = try std.fmt.allocPrint(allocator, "http://{s}:{d}", .{ try client_config.asString("host") orelse unreachable, try client_config.asInt("port") orelse unreachable });
    defer allocator.free(client_uri);

    var client = try Client.init(allocator, client_uri);
    defer client.deinit();

    logger.info().msg("Dymes Data Importer Tool initialized").log();
    try runDataImporterTool(logger, allocator, data_importer_config, &client);
    logger.info().msg("Dymes Data Importer Tool terminating").log();
}

fn parseCmdLine(allocator: Allocator) !ParsedState {
    const params = comptime clap.parseParamsComptime(
        \\-h, --help               Display this help and exit.
        \\--config <str>           Configuration file path.
        \\-l, --loglevel <str>     Logging filter level. One of ("fine", "debug", "info", "warn", "err", "fatal")
        \\-r, --logformat <str>    Logging format. One of ("json", "text")
        \\-i, --input <str>        Specify a file to read and import the data from, must be valid JSON containing a list of message objects
        \\-s, --start <str>        Specify a 26 character encoded ULID (e.g 01K7068MQWRDT23X6TK4QCT5NS) to begin importing from
        \\-H, --host <str>        Dymes node host name
        \\-p, --port <u16>         Dymes node port number
    );

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        var stderr_buffer: [512]u8 = undefined;
        var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
        const stderr = &stderr_writer.interface;
        defer stderr.flush() catch {};
        diag.report(stderr, err) catch {};
        return err;
    };
    defer res.deinit();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const stdin_file = std.fs.File.stdin();
    const stdin_is_tty = stdin_file.isTty();

    if (res.args.help != 0 or args.len == 1 and stdin_is_tty) {
        try clap.helpToFile(.stderr(), clap.Help, &params, .{});
        std.posix.exit(0);
    }

    const config_path = res.args.config orelse "conf/dymes-ddi.yaml";

    const host_override = if (res.args.host) |h| h else null;
    const port_override = if (res.args.port) |p| p else null;

    var dymes_config = try buildBaseConfig(allocator, config_path, host_override, port_override);
    defer dymes_config.deinit();

    const reader_buffer = try allocator.alloc(u8, 4096);
    errdefer allocator.free(reader_buffer);

    var file: std.fs.File = undefined;
    if (res.args.input) |_input_path| {
        std.debug.print("Input file provided: {s}\n", .{_input_path});
        file = try std.fs.cwd().openFile(_input_path, .{});
    } else {
        if (!stdin_is_tty) {
            file = stdin_file;
        } else {
            std.debug.print("stdin is a TTY — no piped input\n", .{});
            std.posix.exit(1);
        }
    }

    const file_reader = try allocator.create(std.fs.File.Reader);
    errdefer allocator.destroy(file_reader);
    file_reader.* = std.fs.File.Reader.init(file, reader_buffer);

    const reader: *std.Io.Reader = &file_reader.interface;

    const start_ulid: Ulid = if (res.args.start) |start_str| blk: {
        const parsed = common.ulid.decode(start_str) catch |err| {
            std.debug.print("Invalid ULID passed to --start: {s}\n", .{start_str});
            return err;
        };
        break :blk parsed;
    } else .{ .time = 0, .rand = 0 };

    return ParsedState.init(
        allocator,
        dymes_config,
        reader,
        file_reader,
        reader_buffer,
        start_ulid,
    );
}

const Header_DTO = struct {
    key: []const u8,
    value: []const u8,
};

const Message_DTO = struct {
    id: []const u8,
    correlation_id: []const u8,
    channel: u32,
    routing: u32,
    headers: []Header_DTO,
    content: []const u8,
};

fn runDataImporterTool(logger: *logging.Logger, allocator: Allocator, parsedState: ParsedState, client: *Client) !void {
    logger.info().msg("Running data importer").log();

    var buffer: [4096]u8 = undefined;

    var json_accumulator = std.ArrayList(u8){};
    defer json_accumulator.deinit(allocator);

    var depth: usize = 0;
    var inside_string = false;
    var escape_next = false;
    var started_array = false;

    while (true) {
        const n = parsedState.reader.readSliceShort(&buffer) catch |err| switch (err) {
            error.ReadFailed => break,
            else => return err,
        };

        if (n == 0) break;
        const chunk = buffer[0..n];

        for (chunk) |c| {
            if (!started_array) {
                if (c == '[') {
                    started_array = true;
                }
                continue;
            }

            if (inside_string) {
                if (escape_next) {
                    escape_next = false;
                } else if (c == '\\') {
                    escape_next = true;
                } else if (c == '"') {
                    inside_string = false;
                }
                try json_accumulator.append(allocator, c);
                continue;
            } else if (c == '"') {
                inside_string = true;
                try json_accumulator.append(allocator, c);
                continue;
            }

            if (c == '{') {
                depth += 1;
            } else if (c == '}') {
                depth -= 1;
            }

            if (depth > 0 or c == '{' or c == '}' or (depth == 0 and json_accumulator.items.len > 0)) {
                try json_accumulator.append(allocator, c);
            }

            if (depth == 0 and c == '}') {
                const obj_slice = json_accumulator.items;

                const parsed = try std.json.parseFromSlice(Message_DTO, allocator, obj_slice, .{});
                defer parsed.deinit();

                const msg = parsed.value;

                const msg_id = common.ulid.decode(msg.id) catch |e| {
                    logger.err().msg("Invalid ULID in 'id' field").err(e).str("id", msg.id).log();
                    json_accumulator.clearRetainingCapacity();
                    continue;
                };

                if (common.util.isBeforeUlid(msg_id, parsedState.start_ulid)) {
                    logger.info()
                        .msg("Skipping message with ULID before start_ulid")
                        .ulid("msg_id", msg_id)
                        .ulid("start_ulid", parsedState.start_ulid)
                        .log();
                    json_accumulator.clearRetainingCapacity();
                    continue;
                }

                const correlation_id: ?Ulid = if (msg.correlation_id.len > 0) blk: {
                    const v = common.ulid.decode(msg.correlation_id) catch |e| {
                        logger.warn()
                            .msg("Invalid correlation ULID; skipping correlation_id")
                            .err(e)
                            .str("correlation_id", msg.correlation_id)
                            .log();
                        break :blk null;
                    };
                    break :blk v;
                } else null;

                const channel_u128 = @as(u128, @intCast(msg.channel));
                const routing_u128 = @as(u128, @intCast(msg.routing));

                var kv_map = std.array_hash_map.StringArrayHashMap([]const u8).init(allocator);
                defer kv_map.deinit();
                for (msg.headers) |h| {
                    kv_map.put(h.key, h.value) catch |e| {
                        logger.err()
                            .msg("Failed to insert header key/value")
                            .err(e)
                            .str("key", h.key)
                            .log();
                    };
                }

                const imported_id = client.importMessage(msg.content, channel_u128, routing_u128, kv_map, correlation_id, msg_id) catch |e| {
                    logger.err()
                        .msg("Failed to import message")
                        .err(e)
                        .ulid("id", msg_id)
                        .intx("channel", channel_u128)
                        .intx("routing", routing_u128)
                        .ulid("correlation_id", correlation_id)
                        .str("body", msg.content)
                        .log();
                    json_accumulator.clearRetainingCapacity();
                    continue;
                };

                logger.info()
                    .msg("Imported message successfully")
                    .ulid("original_id", msg_id)
                    .ulid("imported_id", imported_id)
                    .intx("channel", channel_u128)
                    .intx("routing", routing_u128)
                    .log();

                json_accumulator.clearRetainingCapacity();
            }

            if (started_array and depth == 0 and c == ']') break;
        }
    }

    logger.info().msg("Data Importer Finished").log();
}

fn signalHandler(sig: i32) callconv(.c) void {
    switch (sig) {
        std.posix.SIG.INT, std.posix.SIG.TERM => process_lifecycle.shutdown(),
        else => {},
    }
}

fn buildBaseConfig(allocator: std.mem.Allocator, config_path: []const u8, host_override: ?[]const u8, port_override: ?u16) ConfigError!Config {
    var config_bld = try config.builder("dymes_ddi", allocator);
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
    if (host_override) |host_value| {
        try config_bld.withString("importer.client.host", host_value);
    }
    if (port_override) |port_value| {
        try config_bld.withEntry("importer.client.port", .{ .int_value = port_value });
    }
    return try config_bld.build();
}

fn buildYamlConfig(allocator: std.mem.Allocator, config_path: []const u8) ConfigError!?Config {
    const yaml_cfg = config.fromYamlPath(std.fs.cwd(), config_path, allocator) catch |e| {
        if (e == config.Error.FileNotFound) {
            return null;
        }
        return e;
    };
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

fn parseLoggingFormatName(logging_format_name: []const u8) UsageError!LogFormat {
    if (std.mem.eql(u8, "json", logging_format_name)) {
        return .json;
    } else if (std.mem.eql(u8, "text", logging_format_name)) {
        return .text;
    }
    return UsageError.IllegalArgument;
}

const min_ulid: Ulid = Ulid{
    .time = 0,
    .rand = 0,
};

const max_ulid: Ulid = Ulid{
    .time = 0xFFFFFFFFFFFF,
    .rand = 0xFFFFFFFFFFFF_FFFF_FFFF,
};

const ImporterTestCtx = struct {
    logger: *logging.Logger,
    allocator: Allocator,
    client: *Client,
};

fn ddiWrapper(realTest: *const fn (ctx: ImporterTestCtx) anyerror!void) !void {
    const allocator = std.testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const logger = logging.logger("test.ddi");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    try tmp_dir.dir.makeDir("data-importer-sanity-check");
    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "data-importer-sanity-check");
    defer allocator.free(abs_dir_name);

    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var health = Health.init(allocator);
    defer health.deinit();

    const wrapper_port_no: u16 = prng.random().intRangeAtMost(u16, 40_000, 60_000);

    var cfg_bld = try ConfigBuilder.init("testEngine", allocator);
    defer cfg_bld.deinit();
    try cfg_bld.withInt("engine.store.id", 1234);
    try cfg_bld.withInt("engine.datasets", 1);
    try cfg_bld.withString("engine.store.data_path", abs_dir_name);
    try cfg_bld.withString("engine.http.host", "127.0.0.1");
    try cfg_bld.withInt("engine.http.port", wrapper_port_no);

    var test_dymes_config = try cfg_bld.build();
    defer test_dymes_config.deinit();

    const engine_config = try test_dymes_config.asConfig("engine") orelse return UsageError.OtherUsageFailure;

    var dymes_metrics: DymesMetrics = .init();
    const engine_metrics = &dymes_metrics.engine;
    const http_metrics = &dymes_metrics.http;

    var engine = try Engine.init(allocator, frame_allocator, &health, engine_metrics, engine_config);
    defer engine.deinit();

    var standalone = StandaloneIngester.init(engine);
    var ingester: Ingester = .{ .standalone = &standalone };
    defer ingester.deinit();

    const http_config = try engine_config.asConfig("http") orelse unreachable;
    var server = try Server.init(allocator, engine, &ingester, .{
        .metrics = http_metrics,
        .health = &health,
        .request_address = try http_config.asString("host") orelse unreachable,
        .request_port = @intCast(try http_config.asInt("port") orelse unreachable),
    });
    defer server.deinit();

    std.Thread.sleep(10 * std.time.ns_per_ms);

    var cfg_bld_client = try ConfigBuilder.init("testClient", allocator);
    defer cfg_bld_client.deinit();
    try cfg_bld_client.withString("client.host", "127.0.0.1");
    try cfg_bld_client.withInt("client.port", wrapper_port_no);

    var test_dymes_client_config = try cfg_bld_client.build();
    defer test_dymes_client_config.deinit();

    const client_config = try test_dymes_client_config.asConfig("client") orelse return UsageError.OtherUsageFailure;

    const client_uri = try std.fmt.allocPrint(
        allocator,
        "http://{s}:{d}",
        .{ try client_config.asString("host") orelse unreachable, try client_config.asInt("port") orelse unreachable },
    );
    defer allocator.free(client_uri);

    var client = try Client.init(allocator, client_uri);
    defer client.deinit();

    try realTest(.{
        .logger = logger,
        .allocator = allocator,
        .client = &client,
    });
}

fn runImporterFromJson(ctx: ImporterTestCtx, msgs_json: []const u8, start_ulid_param: Ulid) !void {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    const file_name = "input.json";

    {
        var writer_file = try tmp_dir.dir.createFile(file_name, .{});
        try writer_file.writeAll(msgs_json);
        writer_file.close();
    }

    var in_file = try tmp_dir.dir.openFile(file_name, .{});
    defer in_file.close();

    var ddi_cfg_bld = try ConfigBuilder.init("ddiTestCfg", ctx.allocator);
    defer ddi_cfg_bld.deinit();
    try ddi_cfg_bld.withString("importer.client.host", "127.0.0.1");
    try ddi_cfg_bld.withInt("importer.client.port", 6511);
    var ddi_cfg = try ddi_cfg_bld.build();
    defer ddi_cfg.deinit();

    const reader_buffer = try ctx.allocator.alloc(u8, 4096);
    errdefer ctx.allocator.free(reader_buffer);

    const file_reader = try ctx.allocator.create(std.fs.File.Reader);
    errdefer ctx.allocator.destroy(file_reader);

    file_reader.* = std.fs.File.Reader.init(in_file, reader_buffer);

    const rdr: *std.Io.Reader = &file_reader.interface;

    var parsed_state = try ParsedState.init(
        ctx.allocator,
        ddi_cfg,
        rdr,
        file_reader,
        reader_buffer,
        start_ulid_param,
    );
    defer parsed_state.deinit();

    try runDataImporterTool(ctx.logger, ctx.allocator, parsed_state, ctx.client);
}

fn countAllMessages(ctx: ImporterTestCtx) !usize {
    const query = dymes_msg.Query.range(min_ulid, max_ulid);

    const client_query: ClientQueryDto = switch (query) {
        .query_range => |r| blk: {
            const start = try r.range_start.encodeAlloc(ctx.allocator);
            errdefer ctx.allocator.free(start);

            const end = try r.range_end.encodeAlloc(ctx.allocator);
            errdefer ctx.allocator.free(end);

            break :blk .{
                .query_dto = .{
                    .query_range = .{
                        .range_start = start,
                        .range_end = end,
                    },
                },
                .filters = &[_]dymes_msg.Filter{},
            };
        },
        else => return error.UnsupportedQueryType,
    };

    defer switch (client_query.query_dto) {
        .query_range => |r| {
            ctx.allocator.free(r.range_start);
            ctx.allocator.free(r.range_end);
        },
        else => {},
    };

    var cursor = try ctx.client.openCursor(client_query, .{ .batch_size = 30 }) orelse return error.NoResults;
    defer cursor.close();

    var count: usize = 0;
    while (try cursor.next()) |_| {
        count += 1;
    }
    return count;
}

test "ddi imports single message" {
    try ddiWrapper(&ddiImportsSingleMessageRealTest);
}

fn ddiImportsSingleMessageRealTest(ctx: ImporterTestCtx) !void {
    const base_ms: i64 = std.time.milliTimestamp();

    const id = Ulid{
        .time = @as(u48, @intCast(base_ms)),
        .rand = 0,
    };

    const id_str = try id.encodeAlloc(ctx.allocator);
    defer ctx.allocator.free(id_str);

    const msgs_json = try std.fmt.allocPrint(
        ctx.allocator,
        "[" ++
            "{{\"id\":\"{s}\",\"correlation_id\":\"\",\"channel\":{d},\"routing\":{d},\"headers\":[],\"content\":\"msg\"}}," ++
            "]",
        .{ id_str, 1, 1 },
    );
    defer ctx.allocator.free(msgs_json);

    try runImporterFromJson(ctx, msgs_json, min_ulid);

    const count = try countAllMessages(ctx);
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "ddi imports multiple message" {
    try ddiWrapper(&ddiImportsMultipleMessageRealTest);
}

fn ddiImportsMultipleMessageRealTest(ctx: ImporterTestCtx) !void {
    const base_ms: i64 = std.time.milliTimestamp();

    const first_id = Ulid{
        .time = @as(u48, @intCast(base_ms)),
        .rand = 0,
    };

    const second_id = Ulid{
        .time = @as(u48, @intCast(base_ms + 1)),
        .rand = 0,
    };

    const first_id_str = try first_id.encodeAlloc(ctx.allocator);
    defer ctx.allocator.free(first_id_str);

    const second_id_str = try second_id.encodeAlloc(ctx.allocator);
    defer ctx.allocator.free(second_id_str);

    const msgs_json = try std.fmt.allocPrint(
        ctx.allocator,
        "[" ++
            "{{\"id\":\"{s}\",\"correlation_id\":\"\",\"channel\":{d},\"routing\":{d},\"headers\":[],\"content\":\"msg 1\"}}," ++
            "{{\"id\":\"{s}\",\"correlation_id\":\"\",\"channel\":{d},\"routing\":{d},\"headers\":[],\"content\":\"msg 2\"}}," ++
            "]",
        .{ first_id_str, 1, 1, second_id_str, 1, 1 },
    );
    defer ctx.allocator.free(msgs_json);

    try runImporterFromJson(ctx, msgs_json, min_ulid);

    const count = try countAllMessages(ctx);
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "ddi respects start_ulid and skips older messages" {
    try ddiWrapper(&ddiSkipsOlderMessagesRealTest);
}

fn ddiSkipsOlderMessagesRealTest(ctx: ImporterTestCtx) !void {
    const base_ms: i64 = std.time.milliTimestamp();

    const old_ulid = Ulid{
        .time = @as(u48, @intCast(base_ms)),
        .rand = 0,
    };
    const new_ulid = Ulid{
        .time = @as(u48, @intCast(base_ms + 1)),
        .rand = 0,
    };

    const old_ulid_str = try old_ulid.encodeAlloc(ctx.allocator);
    defer ctx.allocator.free(old_ulid_str);

    const new_ulid_str = try new_ulid.encodeAlloc(ctx.allocator);
    defer ctx.allocator.free(new_ulid_str);

    const msgs_json = try std.fmt.allocPrint(
        ctx.allocator,
        "[" ++
            "{{\"id\":\"{s}\",\"correlation_id\":\"\",\"channel\":{d},\"routing\":{d},\"headers\":[],\"content\":\"old msg\"}}," ++
            "{{\"id\":\"{s}\",\"correlation_id\":\"\",\"channel\":{d},\"routing\":{d},\"headers\":[],\"content\":\"new msg\"}}" ++
            "]",
        .{ old_ulid_str, 1, 1, new_ulid_str, 1, 1 },
    );
    defer ctx.allocator.free(msgs_json);

    try runImporterFromJson(ctx, msgs_json, new_ulid);

    const count = try countAllMessages(ctx);
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "ddi skips messages with invalid id" {
    try ddiWrapper(&ddiSkipsInvalidIdRealTest);
}

fn ddiSkipsInvalidIdRealTest(ctx: ImporterTestCtx) !void {
    const msgs_json = try std.fmt.allocPrint(
        ctx.allocator,
        "[{{\"id\":\"not-a-valid-ulid\",\"correlation_id\":\"\",\"channel\":{d},\"routing\":{d},\"headers\":[],\"content\":\"bad\"}}]",
        .{ 1, 0 },
    );
    defer ctx.allocator.free(msgs_json);

    try runImporterFromJson(ctx, msgs_json, min_ulid);

    const count = try countAllMessages(ctx);
    try std.testing.expectEqual(@as(usize, 0), count);
}
