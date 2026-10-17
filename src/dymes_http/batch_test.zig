//! End-to-end tests of batch append (`POST /api/v1/messages/batch`): an in-process engine, HTTP server and client.
//!
// SPDX-FileCopyrightText: Copyright © 2026 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

const common = @import("dymes_common");
const logging = common.logging;
const ConfigBuilder = common.config.ConfigBuilder;
const Health = common.health.Health;
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;
const UsageError = common.errors.UsageError;

const dymes_engine = @import("dymes_engine");
const Engine = dymes_engine.Engine;
const EngineMetrics = dymes_engine.EngineMetrics;
const FrameAllocator = dymes_engine.FrameAllocator;

const dymes_msg = @import("dymes_msg");
const NIL_CORRELATION_ID = dymes_msg.constants.NIL_CORRELATION_ID;

const dymes_client = @import("dymes_client");
const Client = dymes_client.Client;
const BatchMessage = Client.BatchMessage;

const Server = @import("Server.zig");
const ingest = @import("ingest.zig");
const HttpMetrics = @import("metrics.zig").HttpMetrics;

/// A node on a free port with its store in a temporary folder, and a client of it.
const Node = struct {
    allocator: Allocator,
    tmp_dir: std.testing.TmpDir,
    data_path: []const u8,
    fah: *FrameAllocator,
    health: *Health,
    engine_metrics: *EngineMetrics,
    http_metrics: *HttpMetrics,
    engine: *Engine,
    standalone: *ingest.Standalone,
    ingester: *ingest.Ingester,
    server: *Server,
    client_uri: []const u8,
    client: *Client,

    fn start(allocator: Allocator) !*Node {
        const node = try allocator.create(Node);
        errdefer allocator.destroy(node);
        node.allocator = allocator;
        node.tmp_dir = std.testing.tmpDir(.{ .iterate = true });
        try node.tmp_dir.dir.makeDir("data");
        node.data_path = try node.tmp_dir.dir.realpathAlloc(allocator, "data");

        node.fah = try allocator.create(FrameAllocator);
        node.fah.* = FrameAllocator.init(allocator);
        node.health = try allocator.create(Health);
        node.health.* = Health.init(allocator);
        node.engine_metrics = try allocator.create(EngineMetrics);
        node.engine_metrics.* = .init();
        node.http_metrics = try allocator.create(HttpMetrics);
        node.http_metrics.* = .init();

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp() & 0xFFFF_FFFF));
        const port: u16 = prng.random().intRangeAtMost(u16, 40_000, 60_000);

        var cfg_bld = try ConfigBuilder.init("testBatch", allocator);
        defer cfg_bld.deinit();
        try cfg_bld.withInt("engine.store.id", 1234);
        try cfg_bld.withInt("engine.datasets", 1);
        try cfg_bld.withString("engine.store.data_path", node.data_path);
        var cfg = try cfg_bld.build();
        defer cfg.deinit();
        const engine_config = try cfg.asConfig("engine") orelse return UsageError.OtherUsageFailure;

        node.engine = try Engine.init(allocator, node.fah.allocator(), node.health, node.engine_metrics, engine_config);
        node.standalone = try allocator.create(ingest.Standalone);
        node.standalone.* = ingest.Standalone.init(node.engine);
        node.ingester = try allocator.create(ingest.Ingester);
        node.ingester.* = .{ .standalone = node.standalone };
        node.server = try Server.init(allocator, node.engine, node.ingester, .{
            .metrics = node.http_metrics,
            .health = node.health,
            .request_address = "127.0.0.1",
            .request_port = port,
        });
        std.Thread.sleep(10 * std.time.ns_per_ms);

        node.client_uri = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
        node.client = try allocator.create(Client);
        node.client.* = try Client.init(allocator, node.client_uri);
        return node;
    }

    fn stop(node: *Node) void {
        const allocator = node.allocator;
        node.client.deinit();
        allocator.destroy(node.client);
        allocator.free(node.client_uri);
        node.server.deinit();
        node.ingester.deinit();
        allocator.destroy(node.ingester);
        allocator.destroy(node.standalone);
        node.engine.deinit();
        allocator.destroy(node.http_metrics);
        allocator.destroy(node.engine_metrics);
        node.health.deinit();
        allocator.destroy(node.health);
        allocator.destroy(node.fah);
        allocator.free(node.data_path);
        node.tmp_dir.cleanup();
        allocator.destroy(node);
    }

    /// The stored message's body and correlation id.
    fn expectMessage(node: *Node, id: Ulid, body: []const u8, correlation_id: Ulid) !void {
        // An overlay of the client's response buffer: read it before the next query, and don't free it.
        const message = try node.client.queryMessage(id) orelse return error.TestUnexpectedResult;
        try testing.expectEqualStrings(body, message.msg_body);
        try testing.expectEqual(@as(u128, @bitCast(correlation_id)), @as(u128, @bitCast(message.correlationId())));
    }

    /// Posts a raw JSON body to the batch endpoint and returns the status.
    fn postRaw(node: *Node, json: []const u8) !std.http.Status {
        return node.postRawTo("/api/v1/messages/batch", json);
    }

    /// Posts a raw JSON body to `path` and returns the status.
    fn postRawTo(node: *Node, path: []const u8, json: []const u8) !std.http.Status {
        const url = try std.mem.concat(node.allocator, u8, &.{ node.client_uri, path });
        defer node.allocator.free(url);
        var http_client: std.http.Client = .{ .allocator = node.allocator };
        defer http_client.deinit();
        var sink: std.Io.Writer.Allocating = .init(node.allocator);
        defer sink.deinit();
        const result = try http_client.fetch(.{
            .method = .POST,
            .location = .{ .url = url },
            .payload = json,
            .headers = .{ .content_type = .{ .override = "application/json" } },
            .response_writer = &sink.writer,
        });
        return result.status;
    }
};

fn quietLogging(buffer: []u8) !std.fs.File.Writer {
    logging.default_logging_filter_level = logging.LogLevel.none;
    return std.fs.File.stderr().writer(buffer);
}

fn expectConsecutive(ids: []const Ulid) !void {
    for (ids[1..], 1..) |_id, i| {
        try testing.expect(isBeforeUlid(ids[i - 1], _id));
    }
}

test "batch: in order, consecutive, correlated by index" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    var kv = std.StringArrayHashMap([]const u8).init(allocator);
    defer kv.deinit();
    try kv.put("event_type", "FEED_RECORD");

    const messages = [_]BatchMessage{
        .{ .msg_body = "marker", .channel = 5, .routing = 1 },
        .{ .msg_body = "record 1", .channel = 5, .routing = 2, .kv_map = kv, .correlation_index = 0 },
        .{ .msg_body = "record 2", .channel = 5, .routing = 2, .kv_map = kv, .correlation_index = 0 },
        .{ .msg_body = "record 3", .channel = 6, .routing = 2, .correlation_index = 0 },
        .{ .msg_body = "closed", .channel = 5, .routing = 3, .correlation_index = 0 },
    };
    var ids: [messages.len]Ulid = undefined;
    try node.client.appendMessages(&messages, &ids);

    try expectConsecutive(&ids);
    try node.expectMessage(ids[0], "marker", NIL_CORRELATION_ID);
    try node.expectMessage(ids[1], "record 1", ids[0]);
    try node.expectMessage(ids[2], "record 2", ids[0]);
    try node.expectMessage(ids[3], "record 3", ids[0]);
    try node.expectMessage(ids[4], "closed", ids[0]);
    try testing.expectEqual(@as(u64, 1), node.engine_metrics.engine_batch_appends.get());

    // A later batch may correlate to a stored message by id.
    const later = [_]BatchMessage{.{ .msg_body = "follow-up", .channel = 5, .routing = 2, .correlation_id = ids[0] }};
    var later_ids: [1]Ulid = undefined;
    try node.client.appendMessages(&later, &later_ids);
    try testing.expect(isBeforeUlid(ids[4], later_ids[0]));
    try node.expectMessage(later_ids[0], "follow-up", ids[0]);
}

test "batch: an invalid batch appends nothing" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    var ids: [2]Ulid = undefined;
    // A correlation index must point at an earlier entry.
    const forward = [_]BatchMessage{
        .{ .msg_body = "a", .channel = 5, .routing = 1 },
        .{ .msg_body = "b", .channel = 5, .routing = 1, .correlation_index = 1 },
    };
    try testing.expectError(UsageError.IllegalArgument, node.client.appendMessages(&forward, &ids));
    // A correlation id must be stored.
    const unknown = [_]BatchMessage{
        .{ .msg_body = "a", .channel = 5, .routing = 1 },
        .{ .msg_body = "b", .channel = 5, .routing = 1, .correlation_id = .{ .time = 1, .rand = 1 } },
    };
    try testing.expectError(UsageError.IllegalArgument, node.client.appendMessages(&unknown, &ids));
    // Not both.
    const both = [_]BatchMessage{
        .{ .msg_body = "a", .channel = 5, .routing = 1 },
        .{ .msg_body = "b", .channel = 5, .routing = 1, .correlation_index = 0, .correlation_id = .{ .time = 1, .rand = 1 } },
    };
    try testing.expectError(UsageError.IllegalArgument, node.client.appendMessages(&both, &ids));

    // Nothing was stored: the next batch's first message is the store's first.
    const ok = [_]BatchMessage{.{ .msg_body = "first", .channel = 5, .routing = 1 }};
    var ok_ids: [1]Ulid = undefined;
    try node.client.appendMessages(&ok, &ok_ids);
    try testing.expectEqual(@as(u64, 1), node.engine_metrics.engine_appends_ok.get());
}

test "batch: the node refuses empty and oversized batches" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    try testing.expectEqual(std.http.Status.bad_request, try node.postRaw("{\"messages\":[]}"));

    var json: std.Io.Writer.Allocating = .init(allocator);
    defer json.deinit();
    try json.writer.writeAll("{\"messages\":[");
    for (0..dymes_msg.BatchCreationRequest.max_messages + 1) |i| {
        if (i > 0) try json.writer.writeAll(",");
        try json.writer.writeAll("{\"channel\":5,\"routing\":1,\"encoded_msg_body\":\"aGk\"}");
    }
    try json.writer.writeAll("]}");
    try testing.expectEqual(std.http.Status.bad_request, try node.postRaw(json.written()));
    try testing.expectEqual(@as(u64, 0), node.engine_metrics.engine_appends_ok.get());
}

test "batch: single and batch appends interleave in order" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    const before = try node.client.appendMessage("single 1", 5, 1, null, null);
    const batch = [_]BatchMessage{
        .{ .msg_body = "batch 1", .channel = 5, .routing = 1 },
        .{ .msg_body = "batch 2", .channel = 5, .routing = 1 },
    };
    var ids: [2]Ulid = undefined;
    try node.client.appendMessages(&batch, &ids);
    const after = try node.client.appendMessage("single 2", 5, 1, null, null);

    try expectConsecutive(&[_]Ulid{ before, ids[0], ids[1], after });
    try node.expectMessage(ids[1], "batch 2", NIL_CORRELATION_ID);
}

test "batch: a batch across a segment rollover" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    // A segment rolls over at `max_message_file_entries` messages: fill to just short of it through the engine,
    // then send one batch across the boundary through HTTP.
    const per_segment = @import("dymes_msg_store").limits.max_message_file_entries;
    const chunk = dymes_msg.BatchCreationRequest.max_messages;
    const entries = try allocator.alloc(Engine.BatchEntry, chunk);
    defer allocator.free(entries);
    const fill_ids = try allocator.alloc(Ulid, chunk);
    defer allocator.free(fill_ids);
    var stored: usize = 0;
    while (stored + chunk <= per_segment - 10) {
        for (entries) |*_entry| {
            _entry.* = .{ .channel = 9, .routing = 1, .body = "fill", .options = .{} };
        }
        var written: usize = 0;
        try node.engine.appendBatch(entries, fill_ids, &written);
        stored += written;
    }
    const remaining = per_segment - stored;
    const across_len = remaining + 20;
    const across = try allocator.alloc(BatchMessage, across_len);
    defer allocator.free(across);
    const bodies = try allocator.alloc([]u8, across_len);
    defer {
        for (bodies) |_body| allocator.free(_body);
        allocator.free(bodies);
    }
    for (across, 0..) |*_msg, i| {
        bodies[i] = try std.fmt.allocPrint(allocator, "across {d}", .{i});
        _msg.* = .{ .msg_body = bodies[i], .channel = 7, .routing = 1, .correlation_index = if (i == 0) null else 0 };
    }
    const dataset = &node.engine.storage_subsystem.append_store.dataset;
    const segments_before = dataset.num_segments;
    const ids = try allocator.alloc(Ulid, across_len);
    defer allocator.free(ids);
    try node.client.appendMessages(across, ids);
    try testing.expectEqual(segments_before + 1, dataset.num_segments);

    try expectConsecutive(ids);
    try node.expectMessage(ids[0], "across 0", NIL_CORRELATION_ID);
    try node.expectMessage(ids[remaining - 1], bodies[remaining - 1], ids[0]);
    try node.expectMessage(ids[remaining], bodies[remaining], ids[0]);
    try node.expectMessage(ids[across_len - 1], bodies[across_len - 1], ids[0]);
}

test "batch: an oversized message is refused, and the node stays up" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    // A 3 MiB body: over the 2 MiB frame limit, within the 8 MiB request limit. It stopped the node (a failed
    // assert in the store) until the engine checked the frame size first.
    const raw = try allocator.alloc(u8, 3 * 1024 * 1024);
    defer allocator.free(raw);
    @memset(raw, 'x');
    const encoded = try common.base64.encode(allocator, raw);
    defer allocator.free(encoded);

    const single = try std.fmt.allocPrint(allocator, "{{\"encoded_correlation_id\":null,\"channel\":5,\"routing\":1,\"kv_entries\":[],\"encoded_msg_body\":\"{s}\"}}", .{encoded});
    defer allocator.free(single);
    try testing.expectEqual(std.http.Status.bad_request, try node.postRawTo("/api/v1/messages", single));

    const batch = try std.fmt.allocPrint(allocator, "{{\"messages\":[{{\"channel\":5,\"routing\":1,\"encoded_msg_body\":\"aGk=\"}},{{\"channel\":5,\"routing\":1,\"encoded_msg_body\":\"{s}\"}}]}}", .{encoded});
    defer allocator.free(batch);
    try testing.expectEqual(std.http.Status.bad_request, try node.postRaw(batch));
    try testing.expectEqual(@as(u64, 0), node.engine_metrics.engine_appends_ok.get());

    // Still up, and the largest message that fits is stored.
    const fits = try allocator.alloc(u8, 2 * 1024 * 1024 - 1024);
    defer allocator.free(fits);
    @memset(fits, 'y');
    const id = try node.client.appendMessage(fits, 5, 1, null, null);
    var ids: [1]Ulid = undefined;
    try node.client.appendMessages(&[_]BatchMessage{.{ .msg_body = "after", .channel = 5, .routing = 1 }}, &ids);
    try testing.expect(isBeforeUlid(id, ids[0]));
    try testing.expectEqual(@as(u64, 2), node.engine_metrics.engine_appends_ok.get());
}

test "batch: a body that isn't base64 is the client's error" {
    const allocator = testing.allocator;
    const prev_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_level;
    const buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(buffer);
    var stderr_writer = try quietLogging(buffer);
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const node = try Node.start(allocator);
    defer node.stop();

    try testing.expectEqual(std.http.Status.bad_request, try node.postRaw(
        "{\"messages\":[{\"channel\":5,\"routing\":1,\"encoded_msg_body\":\"not base64!\"}]}",
    ));
    try testing.expectEqual(@as(u64, 0), node.engine_metrics.engine_appends_ok.get());
}
