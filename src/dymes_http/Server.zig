//! Dymes HTTP Server.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const config = common.config;
const Config = common.config.Config;
const default_liveness_endpoint_name = common.net.k8s.default_liveness_endpoint_name;
const default_readiness_endpoint_name = common.net.k8s.default_readiness_endpoint_name;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;

const HttpMetrics = @import("metrics.zig").HttpMetrics;

const resolver = common.net.resolver;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const AllocationError = errors.AllocationError;
const ServerError = errors.ServerError;
const NetworkError = errors.NetworkError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const Logger = logging.Logger;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const MessageBuilder = dymes_msg.MessageBuilder;

const dymes_engine = @import("dymes_engine");
const Engine = dymes_engine.Engine;

const Ingester = @import("ingest.zig").Ingester;

const httpz = @import("httpz");

const constants = @import("constants.zig");

const component_name = "http.Server";

const SharedContext = @import("SharedContext.zig");
const RequestContext = @import("RequestContext.zig");

const Self = @This();

pub const Options = struct {
    request_address: []const u8 = constants.default_request_listen_address,
    request_port: u16 = constants.default_request_listen_port,
    ns_ts_supplier: *const fn () i128 = std.time.nanoTimestamp,
    ms_ts_supplier: *const fn () i64 = std.time.milliTimestamp,
    health: *Health,
    metrics: *HttpMetrics,
};

allocator: Allocator,
logger: *Logger,
shared_ctx: *SharedContext,
nanoTimestamp: *const fn () i128,
httpz_server: httpz.Server(*SharedContext),
listener: std.Thread,
resolved_ip_str: []const u8,

const InitError = CreationError || UsageError || ServerError;

pub fn init(allocator: Allocator, engine: *Engine, ingester: *Ingester, options: Options) InitError!*Self {
    var logger = logging.logger(component_name);
    const new_self = allocator.create(Self) catch return AllocationError.OutOfMemory;
    errdefer allocator.destroy(new_self);

    const resolved_ip_str = resolver.resolveIPv4AsString(allocator, options.request_address) catch |err| {
        logger.err()
            .msg("Failed to resolve listen address")
            .str("listen_address", options.request_address)
            .err(err)
            .log();
        return ServerError.ServerFailure;
    };

    logger.fine()
        .msg("Starting HTTP server")
        .int("listen_port", options.request_port)
        .str("listen_address", resolved_ip_str)
        .log();

    const shared_ctx = try SharedContext.init(allocator, options.metrics, options.health, engine, ingester, .{
        .ns_ts_supplier = options.ns_ts_supplier,
        .ms_ts_supplier = options.ms_ts_supplier,
    });
    errdefer shared_ctx.deinit();

    const httpz_config: httpz.Config = .{
        .port = options.request_port,
        .address = resolved_ip_str,
    };

    var httpz_server = httpz.Server(*SharedContext).init(allocator, httpz_config, shared_ctx) catch |e| {
        logger.err()
            .msg("Failed to start HTTP server")
            .err(e)
            .log();
        return ServerError.ServerFailure;
    };
    errdefer httpz_server.deinit();

    var router = try httpz_server.router(.{});
    router.post(constants.http_path_prefix ++ "/messages", RequestContext.handleCreateMsg, .{});
    router.get(constants.http_path_prefix ++ "/messages/:msg_id", RequestContext.handleQuerySingleMsg, .{});
    router.post(constants.http_path_prefix ++ "/cursors", RequestContext.handleOpenCursor, .{});
    router.get(constants.http_path_prefix ++ "/cursors/:cursor_id", RequestContext.handleTraverseCursor, .{});
    router.delete(constants.http_path_prefix ++ "/cursors/:cursor_id", RequestContext.handleCloseCursor, .{});
    router.post(constants.http_path_prefix ++ "/import", RequestContext.handleImportMsgs, .{});
    router.get(default_liveness_endpoint_name, RequestContext.handleLivenessProbe, .{});
    router.get(default_readiness_endpoint_name, RequestContext.handleReadinessProbe, .{});

    new_self.* = .{
        .allocator = allocator,
        .logger = logging.logger(component_name),
        .shared_ctx = shared_ctx,
        .nanoTimestamp = options.ns_ts_supplier,
        .httpz_server = httpz_server,
        .listener = std.Thread.spawn(.{}, run, .{new_self}) catch return CreationError.OtherCreationFailure,
        .resolved_ip_str = resolved_ip_str,
    };

    const health_provider = ComponentHealthProvider.init(component_name, new_self, healthProbe);
    try options.health.addProvider(health_provider);
    errdefer options.health.removeProvider(component_name);

    defer logger.debug()
        .msg("HTTP server started")
        .int("listen_port", options.request_port)
        .str("listen_address", options.request_address)
        .log();

    return new_self;
}

pub fn deinit(self: *Self) void {
    @atomicStore(bool, &self.shared_ctx.stopping_flag, true, .monotonic);
    defer self.allocator.destroy(self);
    defer self.allocator.free(self.resolved_ip_str);
    defer self.shared_ctx.deinit();
    defer self.httpz_server.deinit();
    defer self.logger.debug()
        .msg("HTTP server stopped")
        .log();
    self.logger.fine()
        .msg("Stopping HTTP server")
        .log();
    self.shared_ctx.health.removeProvider(component_name);
    self.httpz_server.stop();
    self.listener.join();
}

/// Queries the server readiness.
///
/// TODO: Needs fleshing out.
pub fn ready(self: *const Self) bool {
    return !self.shared_ctx.stopping();
}

fn healthProbe(context: *anyopaque) bool {
    var self: *Self = @ptrCast(@alignCast(context));
    return self.ready();
}

fn run(self: *Self) !void {
    std.Thread.sleep(10 * std.time.ns_per_ms);
    self.listener.setName(component_name) catch {};
    self.logger.fine()
        .msg("Started listening for HTTP requests")
        .log();

    self.httpz_server.listen() catch |_e| {
        self.logger.warn()
            .msg("Failure while listening for HTTP requests")
            .err(_e)
            .log();
    };

    self.logger.fine()
        .msg("Stopped listening for HTTP requests")
        .log();
}

const ht = httpz.testing;

const Server = @This();
const Client = @import("dymes_client").Client;

test "Server" {
    std.testing.refAllDeclsRecursive(@This());
}
