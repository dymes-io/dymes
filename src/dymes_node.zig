//! Single Dymes Node Executable.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const io = std.io;
const posix = std.posix;

const builtin = @import("builtin");
const native_os = builtin.os.tag;

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
const AllocationError = errors.AllocationError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const Logger = logging.Logger;

pub const dymes_engine = @import("dymes_engine");
pub const Engine = dymes_engine.Engine;
pub const Query = dymes_engine.Query;
pub const EagerResults = dymes_engine.EagerResults;
pub const QueryRequest = dymes_engine.QueryRequest;
pub const AppendRequest = dymes_engine.AppendRequest;

const Message = dymes_engine.Message;
const FrameAllocator = dymes_engine.FrameAllocator;
const AppendStore = dymes_engine.AppendStore;
const StorageSubsystem = dymes_engine.StorageSubsystem;
const QuerySubsystem = dymes_engine.QuerySubsystem;
const constants = dymes_engine.constants;

const dymes_http = @import("dymes_http");

const EngineMetrics = dymes_engine.EngineMetrics;
const HttpMetrics = dymes_http.HttpMetrics;

const ExporterExitCode = common.metrics.ExporterExitCode;

const DymesMetrics = @import("dymes_metrics.zig").DymesMetrics(EngineMetrics, HttpMetrics);

const Ingester = dymes_http.Ingester;
const StandaloneIngester = dymes_http.StandaloneIngester;
const ClusteredIngester = dymes_http.ClusteredIngester;
const Allocators = dymes_http.Allocators;

const Server = dymes_http.Server;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;
const LifeCycleControl = common.util.LifeCycleControl;

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

    var sa: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0x0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // Run the engine
    {
        var engine_config = try buildConfig(allocator);
        defer engine_config.deinit();

        const out_buffer = try allocator.alloc(u8, 2048);
        defer allocator.free(out_buffer);
        var stderr_writer = std.fs.File.stderr().writer(out_buffer);
        try logging.init(allocator, &stderr_writer);
        defer logging.deinit();

        var logger = logging.logger("dymes_node");

        logger.info()
            .msg("Dymes Node")
            .any("daemon_version", common.constants.dymes_version)
            .log();

        defer logger.info()
            .msg("Dymes Node terminated")
            .log();

        var fah = FrameAllocator.init(allocator);
        const frame_allocator = fah.allocator();

        logger.info()
            .msg("Initializing Dymes Node")
            .log();

        var health = Health.init(allocator);
        defer health.deinit();

        // Prepare mapped metrics
        const dme_mmap_path = try prepDmeMmapPath(logger, allocator, engine_config);
        defer allocator.free(dme_mmap_path);

        const mapper = common.metrics.MetricsMapper(DymesMetrics).init(dme_mmap_path) catch |_e| {
            logger.err()
                .msg("Failed to map metrics exporter data")
                .err(_e)
                .log();
            return AccessError.AccessFailure;
        };
        defer mapper.deinit();

        const dymes_metrics: *DymesMetrics = mapper.metrics;
        dymes_metrics.* = .init();
        const http_metrics = &dymes_metrics.http;
        const engine_metrics = &dymes_metrics.engine;

        // Fire up the engine
        {
            var engine = try Engine.init(
                allocator,
                frame_allocator,
                &health,
                engine_metrics,
                engine_config,
            );
            defer engine.deinit();

            {
                var standalone = StandaloneIngester.init(engine);
                var ingester: Ingester = .{ .standalone = &standalone };
                defer ingester.deinit();

                {
                    const http_config = try engine_config.asConfig("http") orelse unreachable;

                    var server = try Server.init(allocator, engine, &ingester, .{
                        .health = &health,
                        .metrics = http_metrics,
                        .request_address = try http_config.asString("host") orelse unreachable,
                        .request_port = @intCast(try http_config.asInt("port") orelse unreachable),
                    });
                    defer server.deinit();

                    {
                        const args = try std.process.argsAlloc(allocator);
                        defer std.process.argsFree(allocator, args);

                        var dme_runner: DmeRunner = try DmeRunner.init(
                            allocator,
                            args[0],
                            dme_mmap_path,
                            6502,
                        );
                        defer dme_runner.deinit();

                        {
                            logger.info()
                                .msg("Dymes Node ready")
                                .log();

                            while (!process_lifecycle.mustShutdown()) {
                                try dme_runner.monitor();
                                try std.Thread.yield();
                            }

                            logger.info()
                                .msg("Terminating Dymes Node")
                                .log();
                        }
                    }
                }
            }
        }
    }
}

fn prepDmeMmapPath(logger: *Logger, allocator: std.mem.Allocator, engine_config: Config) ![]u8 {
    const store_config = try engine_config.asConfig("store") orelse {
        logger.err()
            .msg("Unable to determine store options from configuration")
            .log();
        return StateError.IllegalState;
    };

    const data_path: []const u8 = try store_config.asString("data_path") orelse return UsageError.IllegalArgument;

    _ = std.fs.cwd().makeOpenPath(data_path, .{ .iterate = true }) catch |e| {
        logger.err()
            .msg("Failed to create data directory")
            .err(e)
            .str("data_path", data_path)
            .log();
        return AccessError.AccessFailure;
    };

    const dme_mmap_path = std.fs.path.join(allocator, &.{ data_path, "dme.mmap" }) catch |_e| {
        logger.err()
            .msg("Failed to prepare metrics exporter mapping path")
            .err(_e)
            .log();
        return AccessError.AccessFailure;
    };
    return dme_mmap_path;
}

pub const DmeRunner = struct {
    const Self = @This();
    const WaitInfo = struct { exited: bool, exit_code: u8, signaled: bool, signal: u8 };
    const child_check_interval: i64 = 10 * std.time.ms_per_s;

    allocator: std.mem.Allocator,
    logger: *Logger,
    exe_path: []u8,
    dme_arg_port: []u8,
    dme_arg_mmap_path: []u8,
    child_pid: ?std.process.Child.Id,
    last_child_check: i64,

    pub fn init(allocator: std.mem.Allocator, bin_path: []const u8, metrics_path: []const u8, port_no: u16) !Self {
        const logger = logging.logger("dymes_node.dme");
        const exe_path = try buildExePath(allocator, bin_path);
        errdefer allocator.free(exe_path);

        const port = try std.fmt.allocPrint(allocator, "--port={d}", .{port_no});
        errdefer allocator.free(port);

        const mmap_path = try std.fmt.allocPrint(allocator, "--file={s}", .{metrics_path});
        errdefer allocator.free(mmap_path);

        return .{
            .allocator = allocator,
            .logger = logger,
            .exe_path = exe_path,
            .dme_arg_port = port,
            .dme_arg_mmap_path = mmap_path,
            .child_pid = null,
            .last_child_check = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        defer self.allocator.free(self.exe_path);
        defer self.allocator.free(self.dme_arg_port);
        defer self.allocator.free(self.dme_arg_mmap_path);
        if (self.child_pid) |_pid| {
            self.logger.fine()
                .msg("Terminating metrics exporter")
                .int("pid", _pid)
                .log();

            posix.kill(_pid, posix.SIG.TERM) catch |_e| {
                if (_e != error.AlreadyTerminated) {
                    self.logger.warn()
                        .msg("Failed to terminate metrics exporter")
                        .err(_e)
                        .int("pid", _pid)
                        .log();
                }
            };
            self.logger.debug()
                .msg("Metrics exporter terminated")
                .int("pid", _pid)
                .log();
        }
    }

    const MonitorFailure = AccessError || CreationError || UsageError;

    pub fn monitor(self: *Self) MonitorFailure!void {
        if (self.child_pid) |_pid| {
            const current_ts = std.time.milliTimestamp();
            if (current_ts - self.last_child_check < child_check_interval) {
                return;
            }
            self.last_child_check = current_ts;

            const waitpid_result = posix.waitpid(_pid, posix.W.NOHANG);

            if (waitpid_result.pid == _pid) {
                // Child went missing
                self.child_pid = null;

                const info = decodeWaitStatus(@as(i32, @bitCast(waitpid_result.status)));
                if (info.exited) {
                    const code = info.exit_code;
                    const eec: ExporterExitCode = @enumFromInt(code);
                    self.logger.warn()
                        .msg("Metrics exporter exited")
                        .intx("rc", code)
                        .str("eec", @tagName(eec))
                        .log();
                    switch (eec) {
                        .ok => {},
                        .usage => return UsageError.IllegalArgument,
                        .listen => return CreationError.DuplicateEntry,
                        .file, .server, .mmap, .write => return AccessError.AccessFailure,
                    }
                }
            }
        } else {
            self.logger.fine()
                .msg("Starting metrics exporter")
                .log();

            const argv = [_][]const u8{ self.exe_path, self.dme_arg_port, self.dme_arg_mmap_path };

            var new_child = std.process.Child.init(&argv, self.allocator);
            new_child.stdout_behavior = .Inherit;
            new_child.stderr_behavior = .Inherit;

            new_child.spawn() catch |_e| {
                self.logger.warn()
                    .msg("Failed to spawn metrics exporter")
                    .err(_e)
                    .log();
                return switch (_e) {
                    error.OutOfMemory => CreationError.OutOfMemory,
                    else => CreationError.OtherCreationFailure,
                };
            };
            new_child.waitForSpawn() catch |_e| {
                self.logger.warn()
                    .msg("Failed to start metrics exporter")
                    .err(_e)
                    .log();
                return switch (_e) {
                    error.OutOfMemory => CreationError.OutOfMemory,
                    else => CreationError.OtherCreationFailure,
                };
            };
            self.logger.debug()
                .msg("Metrics exporter started")
                .int("pid", new_child.id)
                .log();

            self.child_pid = new_child.id;
        }
    }

    fn buildExePath(allocator: std.mem.Allocator, bin_path: []const u8) ![]u8 {
        const dir_rel = std.fs.path.dirname(bin_path) orelse ".";
        const dir_abs = std.fs.cwd().realpathAlloc(allocator, dir_rel) catch {
            return try std.fs.path.join(allocator, &.{ dir_rel, "dymes_dme" });
        };
        defer allocator.free(dir_abs);

        return try std.fs.path.join(allocator, &.{ dir_abs, "dymes_dme" });
    }

    fn decodeWaitStatus(status: i32) WaitInfo {
        const st: u16 = @intCast(status);
        if ((st & 0x7f) == 0) {
            return .{
                .exited = true,
                .exit_code = @intCast((st >> 8) & 0xff),
                .signaled = false,
                .signal = 0,
            };
        } else {
            return .{
                .exited = false,
                .exit_code = 0,
                .signaled = true,
                .signal = @intCast(st & 0x7f),
            };
        }
    }
};

/// Signal handler
///
/// On SIGINT or SIGTERM, it triggers shutdown via lifecycle control
fn signalHandler(sig: c_int) callconv(.c) void {
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
        \\-h, --help             Display this help and exit.
        \\-c, --config <str>     Configuration file path.
        \\-d, --data <str>       Data directory path.
        \\-l, --loglevel <str>   Logging filter level. One of ("fine", "debug", "info", "warn", "err", "fatal")
        \\-r, --logformat <str>  Logging format. One of ("json", "text")
        \\-H, --host <str>       Dymes Node host name (listen interface)
        \\-p, --port <u16>       Dymes Node port number
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
    const config_path = res.args.config orelse constants.default_config_path;
    var dymes_config = try buildBaseConfig(allocator, config_path);
    defer dymes_config.deinit();

    // Prepare logging
    var desired_logging_level: LogLevel = .info;
    if (try dymes_config.asString("engine.logging.level")) |logging_level_name| {
        desired_logging_level = try parseLoggingLevelName(logging_level_name);
    }
    if (res.args.loglevel) |override_level| {
        desired_logging_level = try parseLoggingLevelName(override_level);
    }
    logging.default_logging_filter_level = desired_logging_level;

    var desired_log_format: LogFormat = .text;
    if (try dymes_config.asString("engine.logging.format")) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    // Command line overrides
    if (res.args.logformat) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    logging.default_logging_format = desired_log_format;

    // Engine message store data path
    var engine_data_path = constants.default_engine_data_dir_path;
    if (try dymes_config.asString("engine.store.data_path")) |data_path| {
        engine_data_path = data_path;
    }
    if (res.args.data) |data_path| {
        engine_data_path = data_path;
    }

    // Host
    var host: []const u8 = dymes_http.constants.default_health_listen_address;
    if (try dymes_config.asString("engine.http.host")) |_host| {
        host = _host;
    }
    if (res.args.host) |_host| {
        host = _host;
    }

    // Port
    var port_no: u16 = dymes_http.constants.default_request_listen_port;
    if (try dymes_config.asInt("engine.http.port")) |_port| {
        port_no = @intCast(_port);
    }
    if (res.args.port) |_port| {
        port_no = _port;
    }

    // Build effective engine config
    var engine_config_builder = try config.builder("engine", allocator);
    if (try dymes_config.asConfig("engine")) |base_engine_config| {
        try engine_config_builder.merge(base_engine_config);
    }
    try engine_config_builder.withString("http.host", host);
    try engine_config_builder.withEntry("http.port", .{ .int_value = port_no });
    try engine_config_builder.withString("store.data_path", engine_data_path);
    try engine_config_builder.withString("logging.format", @tagName(desired_log_format));
    try engine_config_builder.withString("logging.level", @tagName(desired_logging_level));
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
    var config_bld = try config.builder("dymes_engine", allocator);
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

// test "dymes_node dependencies" {
//     std.testing.refAllDeclsRecursive(@This());
// }
