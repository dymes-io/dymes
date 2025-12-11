//! Dymes stress client entrypoint.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;
const io = std.io;

const clap = @import("clap");

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;

const HealthProbeListener = common.net.k8s.HealthProbeListener;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;
const LifeCycleControl = common.util.LifeCycleControl;

const dymes_client = @import("dymes_client");

const Client = dymes_client.Client;
const ClientQueryDto = dymes_client.ClientQueryDto;
const FilterDto = dymes_client.FilterDto;

const StressOptions = struct {
    channel: u128 = 702,
    routing: u128 = 66,
    num_messages: usize = 10_000,
    iterations: usize = 1,
    round_delay_ms: u64 = 30_000,
};

// pub const log_level: std.log.Level = .debug;

pub const std_options = std.Options{ .log_level = .warn, .log_scope_levels = &[_]std.log.ScopeLevel{
    .{ .scope = .kv, .level = .info },
    .{ .scope = .net, .level = .info },
} };

var process_lifecycle: LifeCycleControl = .{};

/// Program entry point.
///
pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const semver = common.constants.dymes_version;
    std.debug.print("dymes_stress v{d}.{d}.{d}", .{ semver.major, semver.minor, semver.patch });
    if (semver.pre) |pre| {
        std.debug.print("-{s}\n", .{pre});
    }
    std.debug.print("\n", .{});

    var sa: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0x0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // Run the stress tests
    {
        var stress_config = try buildConfig(allocator);
        defer stress_config.deinit();

        const out_buffer = try allocator.alloc(u8, 2048);
        defer allocator.free(out_buffer);
        var stderr_writer = std.fs.File.stderr().writer(out_buffer);
        try logging.init(allocator, &stderr_writer);
        defer logging.deinit();

        var logger = logging.logger("stress");

        logger.debug()
            .msg("Dymes Stress Client initializing")
            .log();

        var prng_fast = std.Random.DefaultPrng.init(blk: {
            var seed: u64 = undefined;
            std.posix.getrandom(std.mem.asBytes(&seed)) catch |e| {
                logger.err()
                    .msg("Failed to initialize PRNG")
                    .err(e)
                    .log();
                return CreationError.OtherCreationFailure;
            };
            break :blk seed;
        });
        _ = prng_fast.next();
        const channel_id: u128 = @intCast(prng_fast.next());

        var health = Health.init(allocator);
        defer health.deinit();

        {
            const http_config = try stress_config.asConfig("http") orelse unreachable;

            var client = try buildClient(allocator, http_config);
            defer client.deinit();

            logger.debug()
                .msg("Dymes Stress Client initialized")
                .log();

            try stressOut(logger, allocator, &client, .{
                .iterations = @intCast(try stress_config.asInt("iterations") orelse 1),
                .num_messages = @intCast(try stress_config.asInt("num_messages") orelse 10_000),
                .channel = channel_id,
            });

            defer logger.debug()
                .msg("Dymes Stress Client terminating")
                .log();
        }
    }
}

fn buildClient(allocator: Allocator, http_config: Config) !Client {
    const client_uri = try std.fmt.allocPrint(allocator, "http://{s}:{d}", .{ try http_config.asString("host") orelse unreachable, try http_config.asInt("port") orelse unreachable });
    defer allocator.free(client_uri);

    return try Client.init(allocator, client_uri);
}

fn stressOut(logger: *logging.Logger, allocator: Allocator, client: *Client, options: StressOptions) !void {
    logger.info()
        .msg("Stress cycle started")
        .int("total_num_messages", options.num_messages * options.iterations)
        .int("iterations", options.iterations)
        .log();

    const global_progress = std.Progress.start(.{});
    defer global_progress.end();

    var stress_timer = try std.time.Timer.start();
    const stress_start = stress_timer.read();

    {
        var progress = global_progress.start("Stress cycle", options.iterations * 2);
        defer progress.end();

        for (0..options.iterations) |_| {
            const creation_start_ms: u48 = @intCast(std.time.milliTimestamp());
            const ids_to_fetch = try createTestMessages(logger, allocator, client, &progress, options);
            defer allocator.free(ids_to_fetch);

            const creation_end_ms: u48 = @intCast(std.time.milliTimestamp() + std.time.ms_per_s);

            try fetchTestMessages(logger, allocator, client, ids_to_fetch, options.channel, creation_start_ms, creation_end_ms, &progress, options);
            // progress.completeOne();
        }
    }

    const stress_end = stress_timer.read();

    const total_num_messages = options.num_messages * options.iterations;
    const elapsed_ns = stress_end - stress_start;
    const avg_time_ns = elapsed_ns / total_num_messages;

    logger.info()
        .msg("Stress cycle complete")
        .int("total_num_messages", options.num_messages * options.iterations)
        .int("avg_µs", avg_time_ns / std.time.ns_per_us)
        .int("total_µs", elapsed_ns / std.time.ns_per_us)
        .log();
}

fn createTestMessages(logger: *logging.Logger, allocator: std.mem.Allocator, client: *Client, parent_progress: *std.Progress.Node, options: StressOptions) ![]Ulid {
    var progress = parent_progress.start("Creating test messages", options.num_messages);
    defer progress.end();

    const num_messages: usize = options.num_messages;
    logger.debug()
        .msg("Creating test messages")
        .int("num_messages", num_messages)
        .log();
    const msg_body: []const u8 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var msg_idx: usize = 0;
    var all_ids_to_fetch: []Ulid = try allocator.alloc(Ulid, num_messages);
    errdefer allocator.free(all_ids_to_fetch);

    var timer = try std.time.Timer.start();
    const start_time = timer.read();
    while (msg_idx < num_messages and !process_lifecycle.shutdown_trigger) : (msg_idx += 1) {
        all_ids_to_fetch[msg_idx] = try client.appendMessage(msg_body, options.channel, options.routing, null, null);
        progress.completeOne();
    }
    const end_time = timer.read();
    const elapsed_time: u64 = end_time - start_time;
    const avg_time: u64 = elapsed_time / num_messages;

    logger.info()
        .msg("Test messages created")
        .int("num_messages", num_messages)
        .int("avg_µs", avg_time / std.time.ns_per_us)
        .int("total_µs", elapsed_time / std.time.ns_per_us)
        .log();
    return all_ids_to_fetch;
}

fn fetchTestMessages(logger: *logging.Logger, allocator: std.mem.Allocator, client: *Client, msg_ids: []Ulid, channel_id: u128, start_ms: u64, end_ms: u64, parent_progress: *std.Progress.Node, options: StressOptions) !void {
    var progress = parent_progress.start("Fetching test messages", options.num_messages);
    defer progress.end();

    var missing_ids = std.array_hash_map.AutoArrayHashMap(Ulid, void).init(allocator);
    defer missing_ids.deinit();
    for (msg_ids) |msg_id| {
        try missing_ids.put(msg_id, void{});
    }

    const num_messages: usize = options.num_messages;
    logger.debug()
        .msg("Fetching test messages")
        .int("num_messages", missing_ids.count())
        .log();

    var timer = try std.time.Timer.start();

    const range_start: Ulid = .{ .time = @intCast(start_ms), .rand = 0x0 };
    const range_end: Ulid = .{ .time = @intCast(end_ms), .rand = 0x0 };

    const query_dto: ClientQueryDto = .{
        .query_dto = .{
            .query_range = .{
                .range_start = &range_start.encode(),
                .range_end = &range_end.encode(),
            },
        },
        .filters = &.{.{ .channel_filter = options.channel }},
    };

    if (try client.openCursor(query_dto, .{})) |cursor| {
        defer cursor.close();
        while (try cursor.next()) |_msg| {
            assert(_msg.channel() == channel_id);
            _ = missing_ids.swapRemove(_msg.id());
            progress.completeOne();
        }
    }

    if (missing_ids.count() > 0) {
        logger.err()
            .msg("Missing messages detected")
            .int("num_missing", missing_ids.count())
            .log();
    }

    const fetch_time = timer.read();
    const avg_time: u64 = fetch_time / num_messages;

    logger.info()
        .msg("Test messages fetched")
        .int("num_messages", num_messages)
        .int("avg_µs", avg_time / std.time.ns_per_us)
        .int("total_µs", fetch_time / std.time.ns_per_us)
        .log();
}

/// Signal handler
///
/// On SIGINT or SIGTERM, it triggers shutdown via lifecycle control
fn signalHandler(sig: i32) callconv(.c) void {
    switch (sig) {
        std.posix.SIG.INT, std.posix.SIG.TERM => process_lifecycle.shutdown(),
        else => {},
    }
}

/// Builds engine configuration.
///
/// The caller owns the returned config
fn buildConfig(allocator: std.mem.Allocator) !Config {

    // Parse command line
    const params = comptime clap.parseParamsComptime(
        \\-h, --help               Display this help and exit.
        \\-c, --config <str>       Configuration file path.
        \\-d, --data <str>         Data directory path.
        \\-l, --loglevel <str>     Logging filter level. One of ("fine", "debug", "info", "warn", "err", "fatal")
        \\-m, --messages <usize>   Number of messages in stress blocks
        \\-i, --iterations <usize> Number of stress iterations
        \\-r, --logformat <str>    Logging format. One of ("json", "text")
        \\-H, --host <str>         Dymes Node host name (listen interface)
        \\-p, --port <u16>         Dymes Node port number
        \\
    );
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        {
            var stderr_buffer: [1024]u8 = undefined;
            var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
            const stderr = &stderr_writer.interface;
            diag.report(stderr, err) catch {};
            stderr.flush() catch {};
            return err;
        }
    };
    defer res.deinit();
    if (res.args.help != 0) {
        var stderr_buffer: [1024]u8 = undefined;
        var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
        const stderr = &stderr_writer.interface;
        try clap.help(stderr, clap.Help, &params, .{});
        stderr.flush() catch {};
        std.posix.exit(0);
    }

    // Build base dymes configuration
    const config_path = res.args.config orelse "conf/dymes-stress.yaml";
    var dymes_config = try buildBaseConfig(allocator, config_path);
    defer dymes_config.deinit();

    // Prepare logging
    var desired_logging_level: LogLevel = .info;
    if (try dymes_config.asString("stress.logging.level")) |logging_level_name| {
        desired_logging_level = try parseLoggingLevelName(logging_level_name);
    }
    if (res.args.loglevel) |override_level| {
        desired_logging_level = try parseLoggingLevelName(override_level);
    }
    logging.default_logging_filter_level = desired_logging_level;

    var desired_log_format: LogFormat = .text;
    if (try dymes_config.asString("stress.logging.format")) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    // Command line overrides
    if (res.args.logformat) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    logging.default_logging_format = desired_log_format;

    // Host
    var host: []const u8 = "127.0.0.1";
    if (try dymes_config.asString("stress.http.host")) |_host| {
        host = _host;
    }
    if (res.args.host) |_host| {
        host = _host;
    }

    // Port
    var port_no: u16 = dymes_client.constants.default_request_port;
    if (try dymes_config.asInt("stress.http.port")) |_port| {
        port_no = @intCast(_port);
    }
    if (res.args.port) |_port| {
        port_no = _port;
    }

    // Number of messages
    var num_messages: usize = 10_000;
    if (try dymes_config.asInt("stress.num_messages")) |_num_messages| {
        num_messages = @intCast(_num_messages);
    }
    if (res.args.messages) |_num_messages| {
        num_messages = _num_messages;
    }

    // Number of iterations
    var num_iterations: usize = 1;
    if (try dymes_config.asInt("stress.iterations")) |_num_iterations| {
        num_iterations = @intCast(_num_iterations);
    }
    if (res.args.iterations) |_num_iterations| {
        num_iterations = _num_iterations;
    }

    // Build effective engine config
    var engine_config_builder = try config.builder("stress", allocator);
    if (try dymes_config.asConfig("stress")) |base_engine_config| {
        try engine_config_builder.merge(base_engine_config);
    }
    try engine_config_builder.withString("http.host", host);
    try engine_config_builder.withEntry("http.port", .{ .int_value = port_no });
    try engine_config_builder.withString("logging.format", @tagName(desired_log_format));
    try engine_config_builder.withString("logging.level", @tagName(desired_logging_level));
    try engine_config_builder.withEntry("num_messages", .{ .int_value = num_messages });
    try engine_config_builder.withEntry("iterations", .{ .int_value = num_iterations });
    return engine_config_builder.build();
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

const ConfigError = UsageError || CreationError || AccessError;

/// Build base configuration
///
/// First the Dymes YAML config is merged, then environment variable overrides are applied.
fn buildBaseConfig(allocator: std.mem.Allocator, config_path: []const u8) ConfigError!Config {
    var config_bld = try config.builder("dymes_workshop", allocator);
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

/// Build config from Dymes YAML configuration
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

// test "dymes_workshop dependencies" {
//     std.testing.refAllDeclsRecursive(@This());
// }
