//! Dymes workshop entrypoint.
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
const TimeoutError = errors.TimeoutError;
const AllocationError = errors.AllocationError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;

const dymes_engine = @import("dymes_engine");
const Engine = dymes_engine.Engine;
const Query = dymes_engine.Query;
const QueryDto = dymes_engine.QueryDto;
const EagerResults = dymes_engine.EagerResults;
const QueryRequest = dymes_engine.QueryRequest;
const AppendRequest = dymes_engine.AppendRequest;

const Message = dymes_engine.Message;
const FrameAllocator = dymes_engine.FrameAllocator;
const AppendStore = dymes_engine.AppendStore;
const StorageSubsystem = dymes_engine.StorageSubsystem;
const QuerySubsystem = dymes_engine.QuerySubsystem;
const constants = dymes_engine.constants;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;
const LifeCycleControl = common.util.LifeCycleControl;

const dymes_msg = @import("dymes_msg");
const dymes_msg_store = @import("dymes_msg_store");
const dymes_http = @import("dymes_http");
const dymes_client = @import("dymes_client");

const Ingester = dymes_http.Ingester;
const StandaloneIngester = dymes_http.StandaloneIngester;
const ClusteredIngester = dymes_http.ClusteredIngester;
const Allocators = dymes_http.Allocators;

const Server = dymes_http.Server;
const Client = dymes_client.Client;
const ClientQueryDto = dymes_client.ClientQueryDto;

const EngineMetrics = dymes_engine.EngineMetrics;
const HttpMetrics = dymes_http.HttpMetrics;

const DymesMetrics = @import("dymes_metrics.zig").DymesMetrics(EngineMetrics, HttpMetrics);

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
    std.debug.print("dymes_workshop v{d}.{d}.{d}", .{ semver.major, semver.minor, semver.patch });
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

    // Run the engine
    {
        var engine_config = try buildConfig(allocator);
        defer engine_config.deinit();

        const out_buffer = try allocator.alloc(u8, 2048);
        defer allocator.free(out_buffer);
        var stderr_writer = std.fs.File.stderr().writer(out_buffer);
        try logging.init(allocator, &stderr_writer);
        defer logging.deinit();
        var logger = logging.logger("workshop");

        var fah = FrameAllocator.init(allocator);
        const frame_allocator = fah.allocator();

        logger.debug()
            .msg("Dymes Workshop initializing")
            .log();

        var health = Health.init(allocator);
        defer health.deinit();

        var dymes_metrics: DymesMetrics = .init();
        const http_metrics = &dymes_metrics.http;
        const engine_metrics = &dymes_metrics.engine;

        var engine = try Engine.init(allocator, frame_allocator, &health, engine_metrics, engine_config);
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
                std.Thread.sleep(10 * std.time.ns_per_ms);

                logger.info()
                    .msg("Dymes Workshop initialized")
                    .log();

                try runWorkshop(logger, allocator, engine, http_config);

                defer logger.info()
                    .msg("Dymes Workshop terminating")
                    .log();
            }
        }
    }
}

fn runWorkshop(logger: *logging.Logger, allocator: Allocator, engine: *Engine, http_config: Config) !void {
    const client_uri = try std.fmt.allocPrint(allocator, "http://{s}:{d}", .{ try http_config.asString("host") orelse unreachable, try http_config.asInt("port") orelse unreachable });
    defer allocator.free(client_uri);

    var client = try Client.init(allocator, client_uri);
    defer client.deinit();

    // if (!process_lifecycle.mustShutdown()) {
    //     try runCorrelationMinibench(logger, allocator, engine, &client);
    // }

    // if (!process_lifecycle.mustShutdown()) {
    //     try runClientChannelQueries(logger, allocator, engine, &client);
    // }

    // if (!process_lifecycle.mustShutdown()) {
    //     try runChannelQueries(logger, allocator, engine, &client);
    // }

    // if (!process_lifecycle.mustShutdown()) {
    //     try runCorrelationQueries(logger, allocator, engine, &client);
    // }

    // if (!process_lifecycle.mustShutdown()) {
    //     try runRangeQueries(logger, allocator, engine, &client);
    // }

    // if (!process_lifecycle.mustShutdown()) {
    //     try runIdle(logger, allocator, engine, &client);
    // }

    if (!process_lifecycle.mustShutdown()) {
        try runImportQueries(logger, allocator, engine, &client);
    }
}

pub const ImportError = AccessError || CreationError || StateError || TimeoutError || AllocationError;
fn runImportQueries(logger: *logging.Logger, allocator: Allocator, engine: *Engine, client: *Client) !void {
    const msg_buf: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(msg_buf);

    const num_test_messages: usize = 100;
    logger.info()
        .msg("Storing test message blocks")
        .int("num_test_messages", num_test_messages)
        .log();

    const last_msg_id = try storeSampleMessages(client, num_test_messages);

    logger.info()
        .msg("runImportQueries: last generated ULID")
        .any("last_msg_id", last_msg_id)
        .log();

    var preceding_ulid: Ulid = last_msg_id;
    if (preceding_ulid.rand > 0) {
        preceding_ulid.rand -= 1;
    } else if (preceding_ulid.time > 0) {
        preceding_ulid.time -= 1;
        preceding_ulid.rand = std.math.maxInt(u80);
    } else {
        logger.warn()
            .msg("runImportQueries: cannot construct preceding ULID (already minimal)")
            .log();
        return;
    }

    logger.info()
        .msg("runImportQueries: constructed preceding ULID")
        .any("preceding_ulid", preceding_ulid)
        .log();

    const import_result = engine.import(
        preceding_ulid,
        1,
        2,
        "import test message preceding ULID",
        .{
            .correlation_id = .{ .time = 0, .rand = 0 },
            .transient_kv_headers = null,
        },
    );

    //check that OutOfOrderCreation error gets thrown
    if (import_result) |_| {
        return error.UnexpectedSuccess;
    } else |err| {
        if (err != ImportError.OutOfOrderCreation) {
            return err;
        }
    }

    var following_ulid: Ulid = last_msg_id;
    if (following_ulid.rand < std.math.maxInt(u80)) {
        following_ulid.rand += 1;
    } else if (following_ulid.time < std.math.maxInt(u80)) {
        following_ulid.time += 1;
        following_ulid.rand = 0;
    } else {
        logger.warn()
            .msg("runImportQueries: cannot construct following ULID (already max)")
            .log();
        return;
    }

    logger.info()
        .msg("runImportQueries: constructed following ULID")
        .any("following_ulid", following_ulid)
        .log();

    const import_result_next = try engine.import(
        following_ulid,
        1,
        2,
        "import test message following ULID",
        .{
            .correlation_id = .{ .time = 0, .rand = 0 },
            .transient_kv_headers = null,
        },
    );

    assert(following_ulid.time == import_result_next.time);
    assert(following_ulid.rand == import_result_next.rand);
}

fn runCorrelationMinibench(logger: *logging.Logger, allocator: Allocator, engine: *Engine, client: *Client) !void {
    _ = &engine;

    const num_test_messages: usize = 100_000;
    // const num_test_messages: usize = 200_000;
    logger.info()
        .msg("Correlated messages")
        .int("num_test_messages", num_test_messages)
        .log();

    const ids_to_fetch = try storeCorrelatedTestMessageBlock(logger, allocator, client, num_test_messages);
    defer allocator.free(ids_to_fetch);

    {
        var qrb = try QueryRequest.Builder.init(allocator);
        defer qrb.deinit();

        logger.info()
            .msg("Starting correlation chain traversal")
            .int("num_test_messages", num_test_messages)
            .log();

        var correlated_ids: []Ulid = try allocator.alloc(Ulid, ids_to_fetch.len);
        defer allocator.free(correlated_ids);
        var traversal_timer = std.time.Timer.start() catch unreachable;
        {
            // Fetch correlation chain
            const corr_id = ids_to_fetch[ids_to_fetch.len - 1];
            var idx: usize = 0;

            const query_dto: ClientQueryDto = .{
                .query_dto = .{
                    .query_correlation = &corr_id.encode(),
                },
                .filters = &.{},
            };

            if (try client.openCursor(query_dto, .{})) |cursor| {
                defer cursor.close();
                while (try cursor.next()) |_msg| : (idx = idx + 1) {
                    assert(idx < ids_to_fetch.len);
                    correlated_ids[idx] = _msg.id();
                }
            }
        }
        const traversal_time_by_chain = traversal_timer.read();
        std.mem.sort(Ulid, correlated_ids, {}, struct {
            pub fn inner(_: void, lhs: Ulid, rhs: Ulid) bool {
                return common.util.isBeforeUlid(lhs, rhs);
            }
        }.inner);

        assert(std.mem.eql(Ulid, ids_to_fetch, correlated_ids));

        logger.info()
            .msg("Completed correlation chain traversal")
            .int("num_test_messages", num_test_messages)
            .int("avg_traversal_time_µs", (traversal_time_by_chain / num_test_messages) / std.time.ns_per_us)
            .int("traversal_time_ms", traversal_time_by_chain / std.time.ns_per_ms)
            .log();
    }
}

fn runChannelQueries(logger: *logging.Logger, allocator: Allocator, engine: *Engine, _: *Client) !void {
    _ = &allocator;

    // POPULATE
    var ulid_generator = engine.ulid_gen;
    const first_ulid: Ulid = try ulid_generator.next();

    // const number_of_messages: usize = @min((dymes_msg_store.limits.max_message_file_size - 128) / 1024 + 10, dymes_msg_store.limits.max_message_file_entries + 10);
    const number_of_channels = 50000;
    const number_of_messages: usize = number_of_channels * 10;
    var msg_counter: usize = 0;

    var msg_by_channel = std.AutoArrayHashMap(u128, []Ulid).init(allocator);
    defer msg_by_channel.deinit();
    for (1..number_of_channels + 1) |_idx| {
        try msg_by_channel.put(_idx, try allocator.alloc(Ulid, 20));
    }
    defer {
        var msg_bty_channel_it = msg_by_channel.iterator();
        while (msg_bty_channel_it.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
    }

    logger.debug()
        .msg("Will store and channel query messages")
        .int("number_of_messages", number_of_messages)
        .log();

    {
        const message_body = "A" ** 1536;

        logger.debug().msg("Storing block of test messages").log();

        for (1..number_of_channels + 1) |channel_id| {
            var channel_msg_ids = msg_by_channel.get(channel_id).?;
            for (0..10) |_idx| {
                defer msg_counter += 1;
                const msg_id = try engine.append(channel_id, msg_counter, message_body, .{});
                channel_msg_ids[_idx] = msg_id;
            }
        }
        logger.debug()
            .msg("Test messages stored")
            .int("number_of_messages", number_of_messages)
            .int("number_of_segments", engine.query_subsystem.immutable_store.read_dataset.num_segments)
            .log();
    }

    const final_ulid: Ulid = try ulid_generator.next();

    logger.info().msg("Performing channel query").log();

    var last_result_set_ulids: [20]Ulid = undefined;
    var last_result_set_counter: [20]u128 = undefined;
    for (1..number_of_channels + 1) |channel_id| {
        const final_segment = engine.query_subsystem.immutable_store.read_dataset.num_segments - 1;
        var it = try engine.query_subsystem.immutable_store.read_dataset.channelIterator(channel_id, first_ulid, final_ulid, 0, final_segment);
        defer it.close(engine.query_subsystem.immutable_store.read_dataset.frame_allocator);

        var actual_count: usize = 0;
        var idx: usize = 0;
        while (try it.next()) |_msg| : (idx = idx + 1) {
            last_result_set_ulids[idx] = _msg.id();
            last_result_set_counter[idx] = _msg.routing();
            actual_count += 1;
        }
        if (10 != actual_count) {
            logger.warn()
                .msg("Result mismatch")
                .int("channel_id", channel_id)
                .int("expected_count", 10)
                .int("actual_count", actual_count)
                .any("actual_ulids", last_result_set_ulids[0..])
                .any("expected_ulids", msg_by_channel.get(channel_id).?)
                .any("counter", last_result_set_counter[0..])
                .log();
        }
    }
}

fn runClientChannelQueries(logger: *logging.Logger, allocator: Allocator, engine: *Engine, client: *Client) !void {
    _ = &allocator;

    // POPULATE
    var ulid_generator = engine.ulid_gen;
    const first_ulid: Ulid = try ulid_generator.next();

    // const number_of_channels = 50_000; Flaky due to MSG
    const number_of_channels = 20_000;
    const number_of_messages: usize = number_of_channels * 10;
    var msg_counter: usize = 0;

    var msg_by_channel = std.AutoArrayHashMap(u128, []Ulid).init(allocator);
    defer msg_by_channel.deinit();
    for (1..number_of_channels + 1) |_idx| {
        try msg_by_channel.put(_idx, try allocator.alloc(Ulid, 20));
    }
    defer {
        var msg_bty_channel_it = msg_by_channel.iterator();
        while (msg_bty_channel_it.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
    }

    logger.debug()
        .msg("Will store and channel query messages")
        .int("number_of_messages", number_of_messages)
        .log();

    {
        const message_body = "A" ** 1536;

        logger.debug().msg("Storing block of test messages").log();

        for (1..number_of_channels + 1) |channel_id| {
            var channel_msg_ids = msg_by_channel.get(channel_id).?;
            for (0..10) |_idx| {
                defer msg_counter += 1;
                const msg_id = try engine.append(channel_id, msg_counter, message_body, .{});
                channel_msg_ids[_idx] = msg_id;
            }
        }
        logger.debug()
            .msg("Test messages stored")
            .int("number_of_messages", number_of_messages)
            .int("number_of_segments", engine.query_subsystem.immutable_store.read_dataset.num_segments)
            .log();
    }

    const final_ulid: Ulid = try ulid_generator.next();

    logger.info().msg("Performing channel query").log();

    var last_result_set_ulids: [20]Ulid = undefined;
    var last_result_set_counter: [20]u128 = undefined;
    for (1..number_of_channels + 1) |channel_id| {
        const query_dto: ClientQueryDto = .{
            .query_dto = .{ .query_channel = .{
                .range_start = &first_ulid.encode(),
                .range_end = &final_ulid.encode(),
                .channel_id = channel_id,
            } },
        };

        if (try client.openCursor(query_dto, .{})) |cursor| {
            defer cursor.close();

            var actual_count: usize = 0;
            var idx: usize = 0;
            while (try cursor.next()) |_msg| : (idx = idx + 1) {
                last_result_set_ulids[idx] = _msg.id();
                last_result_set_counter[idx] = _msg.routing();
                actual_count += 1;
            }
            if (10 != actual_count) {
                logger.warn()
                    .msg("Result mismatch")
                    .int("channel_id", channel_id)
                    .int("expected_count", 10)
                    .int("actual_count", actual_count)
                    .any("actual_ulids", last_result_set_ulids[0..])
                    .any("expected_ulids", msg_by_channel.get(channel_id).?)
                    .any("counter", last_result_set_counter[0..])
                    .log();
            }
        } else {
            return error.EntryNotFound;
        }
    }
}

fn runCorrelationQueries(logger: *logging.Logger, allocator: Allocator, engine: *Engine, client: *Client) !void {
    _ = &engine;

    const num_test_messages: usize = if (@intFromEnum(logging.default_logging_filter_level) < @intFromEnum(logging.LogLevel.info)) 10 else 10_000;
    logger.info()
        .msg("Correlated messages")
        .int("num_test_messages", num_test_messages)
        .log();

    const ids_to_fetch = try storeCorrelatedTestMessageBlock(logger, allocator, client, num_test_messages);
    defer allocator.free(ids_to_fetch);

    {
        var qrb = try QueryRequest.Builder.init(allocator);
        defer qrb.deinit();

        logger.info()
            .msg("Walking correlated messages \"by hand\"")
            .int("num_test_messages", num_test_messages)
            .log();
        var walk_timer = std.time.Timer.start() catch unreachable;
        {
            // Fetch correlation chain "by hand"
            var correlated_ids: []Ulid = try allocator.alloc(Ulid, ids_to_fetch.len);
            defer allocator.free(correlated_ids);
            var corr_id = ids_to_fetch[ids_to_fetch.len - 1];
            var idx: usize = 0;
            while (corr_id != dymes_msg.constants.NIL_CORRELATION_ID) : (idx = idx + 1) {
                const correlated_msg = try client.queryMessage(corr_id) orelse return StateError.IllegalState;
                defer correlated_msg.deinit(client.frame_allocator);
                correlated_ids[idx] = correlated_msg.id();
                corr_id = correlated_msg.correlationId();
            }
            std.mem.sort(Ulid, correlated_ids, {}, struct {
                pub fn inner(_: void, lhs: Ulid, rhs: Ulid) bool {
                    return common.util.isBeforeUlid(lhs, rhs);
                }
            }.inner);

            assert(std.mem.eql(Ulid, ids_to_fetch, correlated_ids));
        }
        const walk_time_by_hand_ns = walk_timer.read();
        logger.info()
            .msg("Walked correlated messages \"by hand\"")
            .int("num_test_messages", num_test_messages)
            .int("avg_walk_time_µs", (walk_time_by_hand_ns / num_test_messages) / std.time.ns_per_us)
            .int("walk_time_ms", walk_time_by_hand_ns / std.time.ns_per_ms)
            .log();
    }

    {
        var qrb = try QueryRequest.Builder.init(allocator);
        defer qrb.deinit();

        logger.info()
            .msg("Starting correlation chain traversal")
            .int("num_test_messages", num_test_messages)
            .log();
        var traversal_timer = std.time.Timer.start() catch unreachable;
        {
            // Fetch correlation chain
            var correlated_ids: []Ulid = try allocator.alloc(Ulid, ids_to_fetch.len);
            defer allocator.free(correlated_ids);
            const corr_id = ids_to_fetch[ids_to_fetch.len - 1];
            var idx: usize = 0;

            const query_dto: ClientQueryDto = .{
                .query_dto = .{
                    .query_correlation = &corr_id.encode(),
                },
                .filters = &.{},
            };

            if (try client.openCursor(query_dto, .{})) |cursor| {
                logger.debug()
                    .msg("REACHED A")
                    .log();
                defer cursor.close();
                logger.debug()
                    .msg("REACHED B")
                    .log();
                while (try cursor.next()) |_msg| : (idx = idx + 1) {
                    logger.debug()
                        .msg("REACHED C")
                        .log();
                    assert(idx < ids_to_fetch.len);
                    correlated_ids[idx] = _msg.id();
                }
                logger.debug()
                    .msg("REACHED D")
                    .log();
            }

            logger.debug()
                .msg("REACHED E")
                .log();
            std.mem.sort(Ulid, correlated_ids, {}, struct {
                pub fn inner(_: void, lhs: Ulid, rhs: Ulid) bool {
                    return common.util.isBeforeUlid(lhs, rhs);
                }
            }.inner);

            assert(std.mem.eql(Ulid, ids_to_fetch, correlated_ids));
        }
        const traversal_time_by_chain = traversal_timer.read();
        logger.info()
            .msg("Completed correlation chain traversal")
            .int("num_test_messages", num_test_messages)
            .int("avg_traversal_time_µs", (traversal_time_by_chain / num_test_messages) / std.time.ns_per_us)
            .int("traversal_time_ms", traversal_time_by_chain / std.time.ns_per_ms)
            .log();
    }
}

fn runRangeQueries(logger: *logging.Logger, allocator: Allocator, engine: *Engine, client: *Client) !void {
    const msg_buf: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(msg_buf);

    const num_test_message_blocks: usize = if (@intFromEnum(logging.default_logging_filter_level) < @intFromEnum(logging.LogLevel.info)) 1 else 2;
    const num_test_messages_per_block: usize = if (@intFromEnum(logging.default_logging_filter_level) < @intFromEnum(logging.LogLevel.info)) 30 else 30_000;
    logger.info()
        .msg("Storing test message blocks")
        .int("blocks", num_test_message_blocks)
        .int("messages_per_block", num_test_messages_per_block)
        .log();
    var ulid_gen = common.ulid.generator();

    const ids_to_fetch = try storeTestMessageBlocks(logger, allocator, client, num_test_message_blocks, num_test_messages_per_block);
    defer allocator.free(ids_to_fetch);

    var qrb = try QueryRequest.Builder.init(allocator);
    defer qrb.deinit();

    {
        qrb.withQuery(Query.range(ids_to_fetch[0], ids_to_fetch[1]));
        const query_request = try qrb.build();
        defer query_request.deinit();
        logger.info()
            .msg("Performing cold range query")
            .log();

        {
            var cursor = try engine.query(query_request);
            defer cursor.close();
            var walk_timer = std.time.Timer.start() catch unreachable;
            var cold_results_walked: usize = 0;

            while (try cursor.next()) |_| : (cold_results_walked += 1) {}
            const cold_walk_time_ns = walk_timer.read();
            logger.info()
                .msg("Walked cold range query results")
                .int("avg_walk_time_µs", (cold_walk_time_ns / cold_results_walked) / std.time.ns_per_us)
                .int("cold_results_walked", cold_results_walked)
                .int("cold_walk_time_µs", cold_walk_time_ns / std.time.ns_per_us)
                .log();

            walk_timer.reset();
        }

        {
            logger.info()
                .msg("Warm range query via engine directly")
                .log();
            var cursor = try engine.query(query_request);
            defer cursor.close();
            var warm_results_walked: usize = 0;
            var walk_timer = std.time.Timer.start() catch unreachable;
            while (try cursor.next()) |_| : (warm_results_walked += 1) {}
            const warm_walk_time_ns = walk_timer.read();
            logger.info()
                .msg("Walked direct warm range query results")
                .int("avg_walk_time_µs", (warm_walk_time_ns / warm_results_walked) / std.time.ns_per_us)
                .int("warm_results_walked", warm_results_walked)
                .int("warm_walk_time_µs", warm_walk_time_ns / std.time.ns_per_us)
                .log();
        }
    }

    if (!process_lifecycle.mustShutdown()) {
        logger.info()
            .msg("Traversing all messages...")
            .log();
        qrb.withQuery(Query.range(.{ .time = 0, .rand = 0 }, try ulid_gen.next()));
        const query_request = try qrb.build();
        defer query_request.deinit();
        var cursor = try engine.query(query_request);
        defer cursor.close();
        var walk_timer = std.time.Timer.start() catch unreachable;
        var messages_walked: usize = 0;
        while (try cursor.next()) |_| : (messages_walked += 1) {}
        const messages_walk_time_ns = walk_timer.read();
        logger.info()
            .msg("Traversed all messages")
            .int("avg_walk_time_µs", (messages_walk_time_ns / messages_walked) / std.time.ns_per_us)
            .int("messages_walked", messages_walked)
            .int("total_walk_time_µs", messages_walk_time_ns / std.time.ns_per_us)
            .log();
    }

    if (num_test_message_blocks > 1 and !process_lifecycle.mustShutdown()) {
        qrb.withQuery(Query.multiple(ids_to_fetch));
        const query_request = try qrb.build();
        defer query_request.deinit();
        {
            // Cold fetch selected messages
            logger.info()
                .msg("Cold fetching scattered block messages")
                .any("num_messages", ids_to_fetch.len)
                .log();
            var cursor = try engine.query(query_request);
            defer cursor.close();
            var idx: usize = 0;
            while (try cursor.next()) |message| : (idx += 1) {
                assert(@as(u128, @bitCast(message.frame_header.id)) == @as(u128, @bitCast(ids_to_fetch[idx])));
            }
        }
        {
            // WARM fetch selected messages
            logger.info()
                .msg("Warm fetching scattered block messages")
                .any("num_messages", ids_to_fetch.len)
                .log();
            var cursor = try engine.query(query_request);
            defer cursor.close();
            var idx: usize = 0;
            while (try cursor.next()) |message| : (idx += 1) {
                assert(@as(u128, @bitCast(message.frame_header.id)) == @as(u128, @bitCast(ids_to_fetch[idx])));
            }
        }
    }
}

fn runIdle(logger: *logging.Logger, allocator: Allocator, engine: *Engine, client: *Client) !void {
    var message_ids = std.array_list.Managed(Ulid).init(allocator);
    defer message_ids.deinit();

    var qrb = try QueryRequest.Builder.init(allocator);
    defer qrb.deinit();

    var message_no: usize = 0;

    var start_ms: u48 = @intCast(std.time.milliTimestamp());
    while (!process_lifecycle.mustShutdown()) : (message_no += 1) {
        std.Thread.sleep(std.time.ns_per_s * 1);
        logger.debug()
            .msg("Storing an idle message")
            .int("message_no", message_no)
            .log();
        const last_msg_id = try storeIdleMessage(client);
        try message_ids.append(last_msg_id);

        const end_ms: u48 = @intCast(std.time.milliTimestamp());
        const elapsed_ms = end_ms - start_ms;
        logger.info()
            .msg("Stored idle HTTP test message")
            .ulid("msg_id", last_msg_id)
            .log();

        if (elapsed_ms > 10_000) {
            // Fetch messages in last 10 seconds

            logger.info()
                .msg("Range query by time period")
                .int("start_ms", start_ms)
                .int("end_ms", end_ms)
                .log();

            const range_start: Ulid = .{ .time = start_ms, .rand = 0x0 };
            const range_end: Ulid = .{ .time = end_ms, .rand = 0x0 };

            const query_dto: ClientQueryDto = .{
                .query_dto = .{
                    .query_range = .{
                        .range_start = &range_start.encode(),
                        .range_end = &range_end.encode(),
                    },
                },
                // .filters = &range_query_filters,
            };

            if (try client.openCursor(query_dto, .{})) |cursor| {
                defer cursor.close();

                var walk_timer = std.time.Timer.start() catch unreachable;
                var messages_walked: usize = 0;
                while (try cursor.next()) |msg| : (messages_walked += 1) {
                    logger.fine()
                        .msg("Next message from client cursor")
                        .ulid("msg_id", msg.id())
                        .log();
                }
                const messages_walk_time_ns = walk_timer.read();
                if (messages_walked > 0) {
                    logger.info()
                        .msg("Traversed client cursor")
                        .int("avg_walk_time_µs", (messages_walk_time_ns / messages_walked) / std.time.ns_per_us)
                        .int("messages_walked", messages_walked)
                        .int("total_walk_time_µs", messages_walk_time_ns / std.time.ns_per_us)
                        .log();
                } else {
                    logger.warn()
                        .msg("Sadly, traversal failed")
                        .int("total_walk_time_µs", messages_walk_time_ns / std.time.ns_per_us)
                        .log();
                }
            }

            // qrb.withQuery(try Query.timeRange(start_ms, end_ms));
            // const query_request = try qrb.build();
            // defer query_request.deinit();
            // logger.info()
            //     .msg("Range query by time period")
            //     .int("start_ms", start_ms)
            //     .int("end_ms", end_ms)
            //     .log();
            // var cursor = try engine.query(query_request);
            // defer cursor.close();

            start_ms = @as(u48, @intCast(@divFloor(std.time.nanoTimestamp(), std.time.ns_per_ms)));
        }

        if (message_ids.items.len >= 10) {
            logger.debug()
                .msg("Querying idle messages")
                .int("num_idle_messages", message_ids.items.len)
                .log();
            qrb.withQuery(Query.multiple(message_ids.items));
            const query_request = try qrb.build();
            defer query_request.deinit();
            var cursor = try engine.query(query_request);
            defer cursor.close();
            var idx: usize = 0;
            while (try cursor.next()) |message| : (idx += 1) {
                assert(@as(u128, @bitCast(message.frame_header.id)) == @as(u128, @bitCast(message_ids.items[idx])));
            }
            logger.debug()
                .msg("Queried idle messages")
                .int("num_idle_messages", idx)
                .log();
            assert(idx == 10);
            message_ids.clearRetainingCapacity();
        } else {
            logger.info()
                .msg("Querying single idle message")
                .ulid("msg_id", last_msg_id)
                .log();
            if (try client.queryMessage(last_msg_id)) |*message| {
                defer @constCast(message).deinit(client.frame_allocator);
                assert(@as(u128, @bitCast(message.frame_header.id)) == @as(u128, @bitCast(last_msg_id)));
                logger.info()
                    .msg("Queried single idle message")
                    .ulid("msg_id", last_msg_id)
                    .log();
            } else {
                logger.warn()
                    .msg("Single idle message not found")
                    .ulid("msg_id", last_msg_id)
                    .log();
            }
        }
    }
}

fn storeIdleMessage(client: *Client) !Ulid {
    const msg_body: []const u8 = "Idle HTTP test message";
    const msg_id = try client.appendMessage(msg_body, 702, 101, null, null);
    return msg_id;
}

fn storeSampleMessages(client: *Client, num_of_messages: usize) !Ulid {
    const msg_body: []const u8 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var msg_id: Ulid = undefined;
    for (0..num_of_messages) |_| {
        msg_id = try client.appendMessage(msg_body, 702, 101, null, dymes_msg.constants.NIL_CORRELATION_ID);
    }
    return msg_id;
}

fn storeTestMessageBlocks(logger: *logging.Logger, allocator: std.mem.Allocator, client: *Client, num_test_message_blocks: usize, num_test_messages_per_block: usize) ![]Ulid {
    const msg_body: []const u8 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    const third_idx = num_test_messages_per_block / 3;
    const two_third_idx = third_idx * 2;
    var block_no: usize = 0;
    var all_ids_to_fetch: [100]Ulid = undefined;
    var fetch_id_idx: usize = 0;

    while (block_no < num_test_message_blocks and !process_lifecycle.shutdown_trigger) : (block_no += 1) {
        var timer = try std.time.Timer.start();
        var prev_msg_id: Ulid = dymes_msg.constants.NIL_CORRELATION_ID;
        for (0..num_test_messages_per_block) |idx| {
            const msg_id = try client.appendMessage(msg_body, 702, 101, null, prev_msg_id);
            prev_msg_id = msg_id;
            if (idx == third_idx) {
                all_ids_to_fetch[fetch_id_idx] = msg_id;
                fetch_id_idx += 1;
            } else if (idx == two_third_idx) {
                all_ids_to_fetch[fetch_id_idx] = msg_id;
                fetch_id_idx += 1;
            }
        }
        const store_time = timer.read();
        const avg_time: u64 = store_time / num_test_messages_per_block;

        logger.info()
            .msg("Stored test message block")
            .int("avg_µs", avg_time / std.time.ns_per_us)
            .int("block_no", block_no)
            .int("blocks", num_test_message_blocks)
            .int("messages", num_test_messages_per_block)
            .log();
    }
    return allocator.dupe(Ulid, all_ids_to_fetch[0 .. num_test_message_blocks * 2]);
}

fn storeCorrelatedTestMessageBlock(logger: *logging.Logger, allocator: std.mem.Allocator, client: *Client, num_test_messages_per_block: usize) ![]Ulid {
    // const msg_body: []const u8 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    const msg_body: []const u8 = "T" ** 524_160;
    var all_ids_to_fetch: []Ulid = try allocator.alloc(Ulid, num_test_messages_per_block);

    var timer = try std.time.Timer.start();
    var prev_msg_id: Ulid = dymes_msg.constants.NIL_CORRELATION_ID;
    for (0..num_test_messages_per_block) |idx| {
        const msg_id = try client.appendMessage(msg_body, 702, 101, null, prev_msg_id);
        prev_msg_id = msg_id;
        all_ids_to_fetch[idx] = msg_id;
    }
    const store_time = timer.read();
    const avg_time: u64 = store_time / num_test_messages_per_block;

    logger.info()
        .msg("Stored correlated test message block")
        .int("avg_µs", avg_time / std.time.ns_per_us)
        .int("messages", num_test_messages_per_block)
        .log();

    return all_ids_to_fetch;
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
    const config_path = res.args.config orelse "conf/dymes-workshop.yaml";
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
