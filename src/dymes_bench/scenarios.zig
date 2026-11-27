//! Dymes benchmark scenarios: each drives a running node over HTTP against a generated store
//! (see `gen.zig`) and prints one JSON line per measurement.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;

const dymes_client = @import("dymes_client");
const Client = dymes_client.Client;
const ClientQueryDto = dymes_client.ClientQueryDto;
const FilterDto = dymes_client.FilterDto;

const gen = @import("gen.zig");

pub const Options = struct {
    scenario: []const u8,
    url: []const u8,
    manifest: []const u8,
    /// Times each read is repeated
    repeat: usize = 3,
    /// A read of a large channel stops after this many messages
    max_messages: u64 = 50_000,
    /// Seconds each phase of a load scenario lasts
    seconds: u64 = 10,
    /// The node's metrics endpoint
    metrics_url: []const u8 = "http://127.0.0.1:6502/observe/metrics",
    /// Where `crash-load` records what the node acknowledged, for `crash-verify`, and where
    /// `cursors-hold` records the cursors it left open, for `cursors-after`
    state: []const u8 = "",
};

const Sample = struct {
    tracking_id: []const u8,
    messages: u64,
    first_id: []const u8,
    last_id: []const u8,
};

const Channel = struct {
    index: usize,
    id: []const u8,
    messages: u64,
    runs: u64,
    first_id: []const u8,
    last_id: []const u8,
    samples: []const Sample,

    fn channelId(self: Channel) !u128 {
        return std.fmt.parseUnsigned(u128, self.id, 10);
    }
};

const Manifest = struct {
    seed: u64,
    messages: u64,
    frame_bytes: u64,
    start_ms: u64,
    end_ms: u64,
    feed_channels: usize,
    sparse_channel: usize,
    channels: []const Channel,
};

const ulid_min: Ulid = .{ .time = 0, .rand = 0 };
const ulid_max: Ulid = .{ .time = std.math.maxInt(u48), .rand = std.math.maxInt(u80) };

/// A scratch channel outside the generated ones, for scenarios that append.
const scratch_channel: u128 = (@as(u128, 0x06) << 120) | 1;

const ReadResult = struct {
    open_ns: u64,
    first_ns: u64,
    total_ns: u64,
    messages: u64,
};

/// Opens a cursor and reads it to its end, or to `max_messages`.
fn readAll(client: *Client, query: ClientQueryDto, max_messages: u64) !ReadResult {
    var timer = try std.time.Timer.start();
    const maybe_cursor = try client.openCursor(query, .{ .batch_size = 31 });
    const open_ns = timer.read();
    var first_ns: u64 = open_ns;
    var messages: u64 = 0;
    if (maybe_cursor) |cursor| {
        defer cursor.close();
        while (try cursor.next()) |_| {
            if (messages == 0) first_ns = timer.read();
            messages += 1;
            if (messages >= max_messages) break;
        }
    }
    return .{ .open_ns = open_ns, .first_ns = first_ns, .total_ns = timer.read(), .messages = messages };
}

fn channelQuery(channel_id: u128, from: *const [26]u8, to: *const [26]u8, filters: []const FilterDto) ClientQueryDto {
    return .{
        .query_dto = .{ .query_channel = .{ .channel_id = channel_id, .range_start = from, .range_end = to } },
        .filters = filters,
    };
}

fn ms(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_ms;
}

const Out = struct {
    buf: [4096]u8 = undefined,
    writer: std.fs.File.Writer = undefined,

    fn init(self: *Out) void {
        // Streaming: results are appended to a file other processes append to as well
        self.writer = std.fs.File.stdout().writerStreaming(&self.buf);
    }

    fn line(self: *Out, comptime fmt: []const u8, args: anytype) !void {
        try self.writer.interface.print(fmt ++ "\n", args);
        try self.writer.interface.flush();
    }
};

/// The channels the read scenarios use: one with dozens of messages, a mid-sized one, the largest.
const Picks = struct { tiny: Channel, mid: Channel, big: Channel };

fn pick(manifest: Manifest) !Picks {
    var tiny: ?Channel = null;
    for (manifest.channels[0..manifest.feed_channels]) |ch| {
        if (ch.messages >= 10 and ch.messages <= 1000 and ch.samples.len > 0) tiny = ch; // the last such: the rarest
    }
    return .{
        .tiny = tiny orelse return error.NoTinyChannel,
        .mid = manifest.channels[30],
        .big = manifest.channels[0],
    };
}

pub fn run(allocator: Allocator, options: Options) !void {
    const manifest_text = try std.fs.cwd().readFileAlloc(allocator, options.manifest, 64 * 1024 * 1024);
    defer allocator.free(manifest_text);
    const parsed = try std.json.parseFromSlice(Manifest, allocator, manifest_text, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const manifest = parsed.value;

    var out: Out = .{};
    out.init();

    if (std.mem.eql(u8, options.scenario, "channel-read")) {
        try channelRead(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "tracking-lookup")) {
        try trackingLookup(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "sparse-routing")) {
        try sparseRouting(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "newest")) {
        try newest(allocator, options, &out);
    } else if (std.mem.eql(u8, options.scenario, "crash-load")) {
        try crashLoad(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "crash-verify")) {
        try crashVerify(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "disk-full-check")) {
        try diskFullCheck(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "cursors-hold")) {
        try cursorsHold(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "cursors-after")) {
        try cursorsAfter(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "append-storm")) {
        try appendStorm(allocator, options, &out);
    } else if (std.mem.eql(u8, options.scenario, "rollover")) {
        try rollover(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "cursor-leak")) {
        try cursorLeak(allocator, options, manifest, &out);
    } else if (std.mem.eql(u8, options.scenario, "abandoned-traversal")) {
        try abandonedTraversal(allocator, options, true, &out);
    } else if (std.mem.eql(u8, options.scenario, "hung-up-traversal")) {
        try abandonedTraversal(allocator, options, false, &out);
    } else if (std.mem.eql(u8, options.scenario, "mixed-load")) {
        try mixedLoad(allocator, options, manifest, &out);
    } else return error.UnknownScenario;
}

/// A channel read should cost what the channel holds, not what the store holds: whole-channel
/// reads of a tiny, a mid-sized and the largest channel, and the last day of each.
fn channelRead(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    var client = try Client.init(allocator, options.url);
    defer client.deinit();
    const picks = try pick(manifest);

    const last_day: Ulid = .{ .time = @intCast(manifest.end_ms - std.time.ms_per_day), .rand = 0 };
    const cases = [_]struct { name: []const u8, channel: Channel }{
        .{ .name = "tiny", .channel = picks.tiny },
        .{ .name = "mid", .channel = picks.mid },
        .{ .name = "big", .channel = picks.big },
    };
    for (cases) |case| {
        const channel_id = try case.channel.channelId();
        for ([_]struct { name: []const u8, from: Ulid }{ .{ .name = "all", .from = ulid_min }, .{ .name = "last-day", .from = last_day } }) |range| {
            for (0..options.repeat) |attempt| {
                const from = range.from.encode();
                const to = ulid_max.encode();
                const result = try readAll(&client, channelQuery(channel_id, &from, &to, &.{}), options.max_messages);
                const whole = std.mem.eql(u8, range.name, "all") and case.channel.messages <= options.max_messages;
                try out.line("{{\"scenario\":\"channel-read\",\"case\":\"{s}/{s}\",\"attempt\":{d},\"channel\":{d},\"channel_messages\":{d},\"messages\":{d},\"open_ms\":{d:.1},\"first_ms\":{d:.1},\"total_ms\":{d:.1},\"ok\":{}}}", .{
                    case.name,       range.name,         attempt,             case.channel.index,  case.channel.messages,
                    result.messages, ms(result.open_ns), ms(result.first_ns), ms(result.total_ns), !whole or result.messages == case.channel.messages,
                });
            }
        }
    }
}

/// A run's messages found by their `tracking_id` header on the run's channel: over the run's own
/// id range, over the day from its start, and from its start to the store's end.
fn trackingLookup(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    var client = try Client.init(allocator, options.url);
    defer client.deinit();
    const picks = try pick(manifest);

    for ([_]struct { name: []const u8, channel: Channel }{
        .{ .name = "tiny", .channel = picks.tiny },
        .{ .name = "mid", .channel = picks.mid },
        .{ .name = "big", .channel = picks.big },
    }) |case| {
        if (case.channel.samples.len == 0) continue;
        const channel_id = try case.channel.channelId();
        // The middle sample is a run from anywhere in the store's history
        const sample = case.channel.samples[case.channel.samples.len / 2];
        const first = try common.ulid.decode(sample.first_id);
        const last = try common.ulid.decode(sample.last_id);
        const day_end: Ulid = .{ .time = first.time + std.time.ms_per_day, .rand = 0 };
        const filters = [_]FilterDto{.{ .kv_filter = .{ .eql = .{ .key = "tracking_id", .value = sample.tracking_id } } }};

        for ([_]struct { name: []const u8, to: Ulid, complete: bool }{
            .{ .name = "run", .to = last, .complete = true },
            .{ .name = "day", .to = day_end, .complete = last.time < day_end.time },
            .{ .name = "to-end", .to = ulid_max, .complete = true },
        }) |range| {
            for (0..options.repeat) |attempt| {
                const from = first.encode();
                const to = range.to.encode();
                const result = try readAll(&client, channelQuery(channel_id, &from, &to, &filters), std.math.maxInt(u64));
                try out.line("{{\"scenario\":\"tracking-lookup\",\"case\":\"{s}/{s}\",\"attempt\":{d},\"channel\":{d},\"channel_messages\":{d},\"run_messages\":{d},\"messages\":{d},\"open_ms\":{d:.1},\"first_ms\":{d:.1},\"total_ms\":{d:.1},\"ok\":{}}}", .{
                    case.name,       range.name,         attempt,             case.channel.index,  case.channel.messages,                                 sample.messages,
                    result.messages, ms(result.open_ns), ms(result.first_ns), ms(result.total_ns), !range.complete or result.messages == sample.messages,
                });
            }
        }
    }
}

const Appender = struct {
    allocator: Allocator,
    url: []const u8,
    stop: std.atomic.Value(bool) = .init(false),
    /// Latencies in ns; `phase_start` marks where the current phase begins
    latencies: std.array_list.Managed(u64),
    mutex: std.Thread.Mutex = .{},
    failures: u64 = 0,

    fn loop(self: *Appender) void {
        var client = Client.init(self.allocator, self.url) catch return;
        defer client.deinit();
        const body = "x" ** 512;
        while (!self.stop.load(.monotonic)) {
            var timer = std.time.Timer.start() catch return;
            const ok = if (client.appendMessage(body, scratch_channel, 0x1, null, null)) |_| true else |_| false;
            const elapsed = timer.read();
            self.mutex.lock();
            if (ok) self.latencies.append(elapsed) catch {} else self.failures += 1;
            self.mutex.unlock();
            std.Thread.sleep(20 * std.time.ns_per_ms);
        }
    }

    const Stats = struct { count: usize, p50_ms: f64, p99_ms: f64, max_ms: f64 };

    /// Statistics of the appends since the last call, which they are then dropped from.
    fn drain(self: *Appender) Stats {
        self.mutex.lock();
        defer self.mutex.unlock();
        defer self.latencies.clearRetainingCapacity();
        const items = self.latencies.items;
        if (items.len == 0) return .{ .count = 0, .p50_ms = 0, .p99_ms = 0, .max_ms = 0 };
        std.mem.sort(u64, items, {}, std.sort.asc(u64));
        return .{
            .count = items.len,
            .p50_ms = ms(items[items.len / 2]),
            .p99_ms = ms(items[(items.len * 99) / 100]),
            .max_ms = ms(items[items.len - 1]),
        };
    }
};

/// A range read with a routing filter that matches few messages, while another client appends:
/// the appends should not wait for the scan.
fn sparseRouting(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    var appender: Appender = .{ .allocator = allocator, .url = options.url, .latencies = .init(allocator) };
    defer appender.latencies.deinit();
    const thread = try std.Thread.spawn(.{}, Appender.loop, .{&appender});
    defer thread.join();
    defer appender.stop.store(true, .monotonic);

    std.Thread.sleep(options.seconds * std.time.ns_per_s);
    const idle = appender.drain();

    var client = try Client.init(allocator, options.url);
    defer client.deinit();
    const filters = [_]FilterDto{.{ .routing_filter = .{ .variant = .narrowing, .routing_mask = gen.routing_domain_mask, .routing_cmp = gen.routing_sparse_cmp } }};
    const from = ulid_min.encode();
    // Up to the generated store's end: the scratch appends are not part of the count
    const to = (Ulid{ .time = @intCast(manifest.end_ms + 1), .rand = 0 }).encode();
    const result = try readAll(&client, .{
        .query_dto = .{ .query_range = .{ .range_start = &from, .range_end = &to } },
        .filters = &filters,
    }, std.math.maxInt(u64));
    const busy = appender.drain();

    const expected = manifest.channels[manifest.sparse_channel].messages;
    try out.line("{{\"scenario\":\"sparse-routing\",\"store_messages\":{d},\"matches\":{d},\"expected\":{d},\"scan_open_ms\":{d:.1},\"scan_first_ms\":{d:.1},\"scan_total_ms\":{d:.1},\"idle_appends\":{d},\"idle_p50_ms\":{d:.1},\"idle_p99_ms\":{d:.1},\"idle_max_ms\":{d:.1},\"scan_appends\":{d},\"scan_p50_ms\":{d:.1},\"scan_p99_ms\":{d:.1},\"scan_max_ms\":{d:.1},\"append_failures\":{d},\"ok\":{}}}", .{
        manifest.messages, result.messages, expected,          ms(result.open_ns),          ms(result.first_ns), ms(result.total_ns),
        idle.count,        idle.p50_ms,     idle.p99_ms,       idle.max_ms,                 busy.count,          busy.p50_ms,
        busy.p99_ms,       busy.max_ms,     appender.failures, result.messages == expected,
    });
}

/// Ranges a client can send that must never take the node down: a channel query starting at the
/// store's newest message (which must return it), and channel and range queries whose range
/// ends before it starts (which must be refused).
fn newest(allocator: Allocator, options: Options, out: *Out) !void {
    var client = try Client.init(allocator, options.url);
    defer client.deinit();

    const older = try client.appendMessage("older", scratch_channel, 0x1, null, null);
    const id = try client.appendMessage("newest", scratch_channel, 0x1, null, null);
    const from = id.encode();
    const before = older.encode();
    const to = ulid_max.encode();
    const messages: i64 = if (readAll(&client, channelQuery(scratch_channel, &from, &to, &.{}), 10)) |result| @intCast(result.messages) else |_| -1;
    const inverted_channel_refused = if (readAll(&client, channelQuery(scratch_channel, &from, &before, &.{}), 10)) |_| false else |_| true;
    const inverted_range_refused = if (readAll(&client, .{ .query_dto = .{ .query_range = .{ .range_start = &from, .range_end = &before } } }, 10)) |_| false else |_| true;

    std.Thread.sleep(2 * std.time.ns_per_s);
    const alive = probeAppend(allocator, options.url) != null;

    try out.line("{{\"scenario\":\"newest\",\"messages\":{d},\"inverted_channel_refused\":{},\"inverted_range_refused\":{},\"node_alive_after\":{},\"ok\":{}}}", .{
        messages, inverted_channel_refused, inverted_range_refused, alive, messages == 1 and inverted_channel_refused and inverted_range_refused and alive,
    });
}

/// Opens a cursor over plain HTTP and returns its id, without the client library: the scenarios
/// that abandon cursors must not have them closed for them.
fn openRawCursor(allocator: Allocator, http: *std.http.Client, base_url: []const u8, query: ClientQueryDto) !u64 {
    const url = try std.fmt.allocPrint(allocator, "{s}/api/v1/cursors", .{base_url});
    defer allocator.free(url);
    const payload = try std.json.Stringify.valueAlloc(allocator, query, .{ .whitespace = .minified });
    defer allocator.free(payload);
    var body: [64]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&body);
    const result = try http.fetch(.{ .method = .POST, .location = .{ .url = url }, .payload = payload, .response_writer = &response_writer });
    if (result.status != .created) return error.CursorNotOpened;
    return std.fmt.parseUnsigned(u64, response_writer.buffered(), 16);
}

fn deleteRawCursor(allocator: Allocator, http: *std.http.Client, base_url: []const u8, cursor_id: u64) void {
    const url = std.fmt.allocPrint(allocator, "{s}/api/v1/cursors/0x{x}", .{ base_url, cursor_id }) catch return;
    defer allocator.free(url);
    var body: [256]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&body);
    _ = http.fetch(.{ .method = .DELETE, .location = .{ .url = url }, .response_writer = &response_writer }) catch {};
}

/// Sends a cursor fetch and hangs up after `wait_ms` without reading the answer, as a client
/// whose read timed out does.
fn fetchAndHangUp(allocator: Allocator, base_url: []const u8, cursor_id: u64, wait_ms: u64) !void {
    const uri = try std.Uri.parse(base_url);
    var host_buf: [std.Uri.host_name_max]u8 = undefined;
    const host = try uri.getHost(&host_buf);
    const stream = try std.net.tcpConnectToHost(allocator, host, uri.port orelse 80);
    defer stream.close();
    var request_buf: [256]u8 = undefined;
    const request = try std.fmt.bufPrint(&request_buf, "GET /api/v1/cursors/0x{x}?batch_size=0x1f HTTP/1.1\r\nHost: {s}\r\n\r\n", .{ cursor_id, host });
    var sent: usize = 0;
    while (sent < request.len) sent += try std.posix.write(stream.handle, request[sent..]);
    std.Thread.sleep(wait_ms * std.time.ns_per_ms);
}

/// The value of one counter from the node's metrics endpoint, or null if it can't be read.
fn metric(allocator: Allocator, metrics_url: []const u8, name: []const u8) ?i64 {
    var http: std.http.Client = .{ .allocator = allocator };
    defer http.deinit();
    var body: [16 * 1024]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&body);
    const result = http.fetch(.{ .method = .GET, .location = .{ .url = metrics_url }, .response_writer = &response_writer }) catch return null;
    if (result.status != .ok) return null;
    var lines = std.mem.tokenizeScalar(u8, response_writer.buffered(), '\n');
    while (lines.next()) |text| {
        var parts = std.mem.tokenizeScalar(u8, text, ' ');
        const key = parts.next() orelse continue;
        if (!std.mem.eql(u8, key, name)) continue;
        return std.fmt.parseInt(i64, parts.next() orelse return null, 10) catch null;
    }
    return null;
}

/// Times one append on a fresh connection: how long the node takes to answer a new client.
fn probeAppend(allocator: Allocator, url: []const u8) ?f64 {
    var client = Client.init(allocator, url) catch return null;
    defer client.deinit();
    var timer = std.time.Timer.start() catch return null;
    _ = client.appendMessage("probe", scratch_channel, 0x1, null, null) catch return null;
    return ms(timer.read());
}

/// Clients that open cursors, read a batch and go away without closing them: the node should
/// not keep their cursors for ever.
fn cursorLeak(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    const picks = try pick(manifest);
    const channel_id = try picks.big.channelId();
    const from = ulid_min.encode();
    const to = ulid_max.encode();
    const cursors: usize = 32;
    {
        var http: std.http.Client = .{ .allocator = allocator };
        defer http.deinit();
        for (0..cursors) |_| {
            const cursor_id = try openRawCursor(allocator, &http, options.url, channelQuery(channel_id, &from, &to, &.{}));
            fetchAndHangUp(allocator, options.url, cursor_id, 50) catch {};
        }
    }
    const open_at_once = metric(allocator, options.metrics_url, "http_cursors_open") orelse -1;
    std.Thread.sleep(options.seconds * std.time.ns_per_s);
    const open_later = metric(allocator, options.metrics_url, "http_cursors_open") orelse -1;
    try out.line("{{\"scenario\":\"cursor-leak\",\"abandoned\":{d},\"open_at_once\":{d},\"waited_s\":{d},\"open_later\":{d},\"ok\":{}}}", .{
        cursors, open_at_once, options.seconds, open_later, open_later == 0,
    });
}

const Abandoner = struct {
    allocator: Allocator,
    url: []const u8,
    /// Whether the client closes its cursor after hanging up, or just goes away
    closes: bool = true,
    stop: std.atomic.Value(bool) = .init(false),
    rounds: std.atomic.Value(u64) = .init(0),

    /// Opens a filtered read of the whole store that matches nothing, starts a fetch, hangs up
    /// and (if it `closes`) closes the cursor from another connection while the node is still
    /// traversing.
    fn loop(self: *Abandoner) void {
        var http: std.http.Client = .{ .allocator = self.allocator };
        defer http.deinit();
        const from = ulid_min.encode();
        const to = ulid_max.encode();
        const filters = [_]FilterDto{.{ .kv_filter = .{ .eql = .{ .key = "tracking_id", .value = "no-such-run" } } }};
        while (!self.stop.load(.monotonic)) {
            const cursor_id = openRawCursor(self.allocator, &http, self.url, .{
                .query_dto = .{ .query_range = .{ .range_start = &from, .range_end = &to } },
                .filters = &filters,
            }) catch {
                std.Thread.sleep(200 * std.time.ns_per_ms);
                continue;
            };
            fetchAndHangUp(self.allocator, self.url, cursor_id, 200) catch {};
            if (self.closes) deleteRawCursor(self.allocator, &http, self.url, cursor_id);
            _ = self.rounds.fetchAdd(1, .monotonic);
        }
    }
};

/// Clients that time out on a long filtered read and close its cursor while the node is still
/// traversing (`abandoned-traversal`), or hang up and leave the cursor (`hung-up-traversal`). Once
/// they stop, the node should answer a new client promptly; a node still
/// traversing for clients that have gone does not.
fn abandonedTraversal(allocator: Allocator, options: Options, closes: bool, out: *Out) !void {
    const before = probeAppend(allocator, options.url) orelse -1;

    const clients = 4;
    var abandoners: [clients]Abandoner = undefined;
    var threads: [clients]std.Thread = undefined;
    for (&abandoners, &threads) |*abandoner, *thread| {
        abandoner.* = .{ .allocator = allocator, .url = options.url, .closes = closes };
        thread.* = try std.Thread.spawn(.{}, Abandoner.loop, .{abandoner});
    }
    std.Thread.sleep(options.seconds * std.time.ns_per_s);
    for (&abandoners) |*abandoner| abandoner.stop.store(true, .monotonic);
    // The clients are not joined before the probes: one may be waiting on the node

    var rounds: u64 = 0;
    for (&abandoners) |*abandoner| rounds += abandoner.rounds.load(.monotonic);
    var probe_timer = try std.time.Timer.start();
    const after = probeAppend(allocator, options.url) orelse -1;
    const recovered_s = probe_timer.read() / std.time.ns_per_s;
    const open_cursors = metric(allocator, options.metrics_url, "http_cursors_open") orelse -1;
    try out.line("{{\"scenario\":\"{s}\",\"clients\":{d},\"load_s\":{d},\"rounds\":{d},\"probe_before_ms\":{d:.1},\"probe_after_ms\":{d:.1},\"answered_after_s\":{d},\"open_cursors_after\":{d},\"ok\":{}}}", .{
        options.scenario, clients, options.seconds, rounds, before, after, recovered_s, open_cursors, after >= 0 and after < 1000,
    });
    for (&threads) |*thread| thread.join();
}

const Reader = struct {
    const Kind = enum { tail, lookup, sparse };

    allocator: Allocator,
    url: []const u8,
    kind: Kind,
    channel_id: u128 = 0,
    sample: ?Sample = null,
    from_ms: u64 = 0,
    stop: *std.atomic.Value(bool),
    latencies: std.array_list.Managed(u64),
    failures: u64 = 0,

    fn loop(self: *Reader) void {
        var client = Client.init(self.allocator, self.url) catch return;
        defer client.deinit();
        const to = ulid_max.encode();
        while (!self.stop.load(.monotonic)) {
            const result = switch (self.kind) {
                // A consumer following one channel: what arrived in the last hour of the store
                .tail => blk: {
                    const from = (Ulid{ .time = @intCast(self.from_ms), .rand = 0 }).encode();
                    break :blk readAll(&client, channelQuery(self.channel_id, &from, &to, &.{}), 2000);
                },
                // A repair: a run's messages by tracking id, over the day from its start
                .lookup => blk: {
                    const sample = self.sample.?;
                    const first = common.ulid.decode(sample.first_id) catch return;
                    const from = first.encode();
                    const day_end = (Ulid{ .time = first.time + std.time.ms_per_day, .rand = 0 }).encode();
                    const filters = [_]FilterDto{.{ .kv_filter = .{ .eql = .{ .key = "tracking_id", .value = sample.tracking_id } } }};
                    break :blk readAll(&client, channelQuery(self.channel_id, &from, &day_end, &filters), std.math.maxInt(u64));
                },
                // A consumer of a sparse routing domain, over the last six hours of the store
                .sparse => blk: {
                    const from = (Ulid{ .time = @intCast(self.from_ms), .rand = 0 }).encode();
                    const filters = [_]FilterDto{.{ .routing_filter = .{ .variant = .narrowing, .routing_mask = gen.routing_domain_mask, .routing_cmp = gen.routing_sparse_cmp } }};
                    break :blk readAll(&client, .{ .query_dto = .{ .query_range = .{ .range_start = &from, .range_end = &to } }, .filters = &filters }, std.math.maxInt(u64));
                },
            };
            if (result) |read| self.latencies.append(read.total_ns) catch {} else |_| self.failures += 1;
            std.Thread.sleep(500 * std.time.ns_per_ms);
        }
    }
};

/// The load a feed-ingestion deployment puts on one node at once: an appender, 23 consumers
/// each following a channel, two repairs looking runs up by tracking id, and one consumer of a
/// sparse routing domain. Appends should stay fast throughout.
fn mixedLoad(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    var appender: Appender = .{ .allocator = allocator, .url = options.url, .latencies = .init(allocator) };
    defer appender.latencies.deinit();
    const append_thread = try std.Thread.spawn(.{}, Appender.loop, .{&appender});
    std.Thread.sleep(@as(u64, @min(options.seconds, 5)) * std.time.ns_per_s);
    const idle = appender.drain();

    var stop: std.atomic.Value(bool) = .init(false);
    const tails = 23;
    const lookups = 2;
    var readers: [tails + lookups + 1]Reader = undefined;
    var threads: [tails + lookups + 1]std.Thread = undefined;
    const lookup_channels = [lookups]usize{ 0, 30 };
    for (&readers, 0..) |*reader, i| {
        reader.* = if (i < tails) .{
            .allocator = allocator,
            .url = options.url,
            .kind = .tail,
            .channel_id = try manifest.channels[i].channelId(),
            .from_ms = manifest.end_ms - std.time.ms_per_hour,
            .stop = &stop,
            .latencies = .init(allocator),
        } else if (i < tails + lookups) blk: {
            const channel = manifest.channels[lookup_channels[i - tails]];
            break :blk .{
                .allocator = allocator,
                .url = options.url,
                .kind = .lookup,
                .channel_id = try channel.channelId(),
                .sample = channel.samples[channel.samples.len / 2],
                .stop = &stop,
                .latencies = .init(allocator),
            };
        } else .{
            .allocator = allocator,
            .url = options.url,
            .kind = .sparse,
            .from_ms = manifest.end_ms - 6 * std.time.ms_per_hour,
            .stop = &stop,
            .latencies = .init(allocator),
        };
    }
    defer for (&readers) |*reader| reader.latencies.deinit();
    for (&readers, &threads) |*reader, *thread| thread.* = try std.Thread.spawn(.{}, Reader.loop, .{reader});

    std.Thread.sleep(options.seconds * std.time.ns_per_s);
    stop.store(true, .monotonic);
    for (&threads) |*thread| thread.join();
    const busy = appender.drain();
    appender.stop.store(true, .monotonic);
    append_thread.join();

    try out.line("{{\"scenario\":\"mixed-load\",\"phase\":\"appends\",\"load_s\":{d},\"idle_appends\":{d},\"idle_p50_ms\":{d:.1},\"idle_p99_ms\":{d:.1},\"idle_max_ms\":{d:.1},\"load_appends\":{d},\"load_p50_ms\":{d:.1},\"load_p99_ms\":{d:.1},\"load_max_ms\":{d:.1},\"append_failures\":{d},\"ok\":{}}}", .{
        options.seconds,                                             idle.count, idle.p50_ms, idle.p99_ms, idle.max_ms, busy.count, busy.p50_ms, busy.p99_ms, busy.max_ms, appender.failures,
        busy.count > 0 and busy.p99_ms < 100 and busy.max_ms < 1000,
    });
    for ([_]Reader.Kind{ .tail, .lookup, .sparse }) |kind| {
        var all = std.array_list.Managed(u64).init(allocator);
        defer all.deinit();
        var failures: u64 = 0;
        for (&readers) |*reader| {
            if (reader.kind != kind) continue;
            try all.appendSlice(reader.latencies.items);
            failures += reader.failures;
        }
        std.mem.sort(u64, all.items, {}, std.sort.asc(u64));
        const n = all.items.len;
        try out.line("{{\"scenario\":\"mixed-load\",\"phase\":\"{s}-reads\",\"reads\":{d},\"p50_ms\":{d:.1},\"p99_ms\":{d:.1},\"max_ms\":{d:.1},\"failures\":{d}}}", .{
            @tagName(kind),                          n,
            if (n == 0) 0 else ms(all.items[n / 2]), if (n == 0) 0 else ms(all.items[(n * 99) / 100]),
            if (n == 0) 0 else ms(all.items[n - 1]), failures,
        });
    }
}

/// Appends more messages than a segment holds, on two channels, so the node rolls over to a new
/// segment at least once while serving, then reads both channels back across the rollover and a
/// generated channel as before: every message once, in order.
fn rollover(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    var client = try Client.init(allocator, options.url);
    defer client.deinit();
    const picks = try pick(manifest);

    const from_start = ulid_min.encode();
    const to_end = ulid_max.encode();
    const mid_id = try picks.mid.channelId();
    const mid_before = try readAll(&client, channelQuery(mid_id, &from_start, &to_end, &.{}), std.math.maxInt(u64));

    const batch_len = 100;
    const total: u64 = 300_000;
    const second_channel = scratch_channel + 1;
    var batch: [batch_len]Client.BatchMessage = undefined;
    var ids: [batch_len]Ulid = undefined;
    var first_id: ?Ulid = null;
    var appended: [2]u64 = .{ 0, 0 };
    var written: u64 = 0;
    var timer = try std.time.Timer.start();
    while (written < total) : (written += batch_len) {
        for (&batch, 0..) |*message, i| {
            // One message in seven goes to the second channel
            const second = (written + i) % 7 == 3;
            message.* = .{ .msg_body = "r" ** 256, .channel = if (second) second_channel else scratch_channel, .routing = 0x1 };
            appended[@intFromBool(second)] += 1;
        }
        try client.appendMessages(&batch, &ids);
        if (first_id == null) first_id = ids[0];
    }
    const append_ns = timer.read();

    const from = first_id.?.encode();
    var read: [2]u64 = .{ 0, 0 };
    var in_order = true;
    for ([_]u128{ scratch_channel, second_channel }, 0..) |channel, which| {
        const maybe_cursor = try client.openCursor(channelQuery(channel, &from, &to_end, &.{}), .{ .batch_size = 31 });
        if (maybe_cursor) |cursor| {
            defer cursor.close();
            var previous: Ulid = ulid_min;
            while (try cursor.next()) |message| {
                if (!common.util.isBeforeUlid(previous, message.id()) or message.channel() != channel) in_order = false;
                previous = message.id();
                read[which] += 1;
            }
        }
    }
    const mid_after = try readAll(&client, channelQuery(mid_id, &from_start, &to_end, &.{}), std.math.maxInt(u64));

    try out.line("{{\"scenario\":\"rollover\",\"appended\":{d},\"append_s\":{d},\"first_channel\":{d},\"first_channel_read\":{d},\"second_channel\":{d},\"second_channel_read\":{d},\"in_order\":{},\"generated_channel_before\":{d},\"generated_channel_after\":{d},\"ok\":{}}}", .{
        total,                                                                                                                                                      append_ns / std.time.ns_per_s, appended[0], read[0], appended[1], read[1], in_order, mid_before.messages, mid_after.messages,
        read[0] == appended[0] and read[1] == appended[1] and in_order and mid_before.messages == mid_after.messages and mid_before.messages == picks.mid.messages,
    });
}

const crash_writers = 4;

fn crashChannel(writer: usize) u128 {
    return scratch_channel + 16 + writer;
}

const CrashWriter = struct {
    allocator: Allocator,
    url: []const u8,
    channel: u128,
    batches: bool,
    acked: std.array_list.Managed(Ulid),

    /// Appends until the node stops answering, keeping every id the node acknowledged.
    fn loop(self: *CrashWriter) void {
        var client = Client.init(self.allocator, self.url) catch return;
        defer client.deinit();
        var batch: [25]Client.BatchMessage = undefined;
        for (&batch) |*message| message.* = .{ .msg_body = "c" ** 300, .channel = self.channel, .routing = 0x1 };
        var ids: [25]Ulid = undefined;
        while (true) {
            if (self.batches) {
                client.appendMessages(&batch, &ids) catch return;
                self.acked.appendSlice(&ids) catch return;
            } else {
                const id = client.appendMessage("c" ** 300, self.channel, 0x1, null, null) catch return;
                self.acked.append(id) catch return;
            }
        }
    }
};

/// Appends on four channels (two a message at a time, two in batches) and reads a large channel,
/// until the node goes away: the caller kills it mid-append and mid-traversal. Records every id
/// the node acknowledged in `--state`, for `crash-verify` to check after the node is restarted.
fn crashLoad(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    if (options.state.len == 0) return error.StateFileRequired;
    var writers: [crash_writers]CrashWriter = undefined;
    var threads: [crash_writers]std.Thread = undefined;
    for (&writers, &threads, 0..) |*writer, *thread, i| {
        writer.* = .{ .allocator = allocator, .url = options.url, .channel = crashChannel(i), .batches = i >= 2, .acked = .init(allocator) };
        thread.* = try std.Thread.spawn(.{}, CrashWriter.loop, .{writer});
    }
    defer for (&writers) |*writer| writer.acked.deinit();

    // A traversal in flight when the node dies
    var read: u64 = 0;
    {
        var client = try Client.init(allocator, options.url);
        defer client.deinit();
        const from = ulid_min.encode();
        const to = ulid_max.encode();
        const big = try manifest.channels[0].channelId();
        if (client.openCursor(channelQuery(big, &from, &to, &.{}), .{ .batch_size = 31 }) catch null) |cursor| {
            while (cursor.next() catch null) |_| read += 1;
        }
    }
    for (&threads) |*thread| thread.join();

    // Added to what earlier rounds recorded: each check covers every round so far
    var file = try std.fs.cwd().createFile(options.state, .{ .truncate = false });
    defer file.close();
    var buf: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(&buf);
    try file_writer.seekTo(try file.getEndPos());
    var total: usize = 0;
    for (&writers, 0..) |*writer, i| {
        for (writer.acked.items) |id| try file_writer.interface.print("{d} {s}\n", .{ i, id.encode() });
        total += writer.acked.items.len;
    }
    try file_writer.interface.flush();
    try out.line("{{\"scenario\":\"crash-load\",\"acknowledged\":{d},\"read_before_crash\":{d}}}", .{ total, read });
}

/// After a restart: every append the node acknowledged before it was killed is there, once, in
/// order; the node appends again; a generated channel reads as it did.
fn crashVerify(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    if (options.state.len == 0) return error.StateFileRequired;
    const text = try std.fs.cwd().readFileAlloc(allocator, options.state, 1024 * 1024 * 1024);
    defer allocator.free(text);

    var acked: [crash_writers]std.array_list.Managed(Ulid) = undefined;
    for (&acked) |*list| list.* = .init(allocator);
    defer for (&acked) |*list| list.deinit();
    var lines = std.mem.tokenizeScalar(u8, text, '\n');
    while (lines.next()) |line_text| {
        var parts = std.mem.tokenizeScalar(u8, line_text, ' ');
        const writer = try std.fmt.parseUnsigned(usize, parts.next() orelse continue, 10);
        try acked[writer].append(try common.ulid.decode(parts.next() orelse continue));
    }

    var client = try Client.init(allocator, options.url);
    defer client.deinit();
    const from = ulid_min.encode();
    const to = ulid_max.encode();
    var acknowledged: u64 = 0;
    var missing: u64 = 0;
    var unacknowledged: u64 = 0;
    var out_of_order: u64 = 0;
    for (&acked, 0..) |*list, i| {
        acknowledged += list.items.len;
        var next_acked: usize = 0;
        var previous: Ulid = ulid_min;
        const maybe_cursor = try client.openCursor(channelQuery(crashChannel(i), &from, &to, &.{}), .{ .batch_size = 31 });
        if (maybe_cursor) |cursor| {
            defer cursor.close();
            while (try cursor.next()) |message| {
                const id = message.id();
                if (!common.util.isBeforeUlid(previous, id)) out_of_order += 1;
                previous = id;
                // Acknowledged ids were recorded in order; skip those the store does not hold
                while (next_acked < list.items.len and common.util.isBeforeUlid(list.items[next_acked], id)) {
                    next_acked += 1;
                    missing += 1;
                }
                if (next_acked < list.items.len and list.items[next_acked].equals(id)) {
                    next_acked += 1;
                } else {
                    // Stored, but the node died before it could say so
                    unacknowledged += 1;
                }
            }
        }
        missing += list.items.len - next_acked;
    }

    const appends_again = probeAppend(allocator, options.url) != null;
    const picks = try pick(manifest);
    const mid = try readAll(&client, channelQuery(try picks.mid.channelId(), &from, &to, &.{}), std.math.maxInt(u64));

    try out.line("{{\"scenario\":\"crash-verify\",\"acknowledged\":{d},\"missing\":{d},\"out_of_order\":{d},\"stored_unacknowledged\":{d},\"appends_again\":{},\"generated_channel\":{d},\"generated_channel_expected\":{d},\"ok\":{}}}", .{
        acknowledged,                                                                                                     missing, out_of_order, unacknowledged, appends_again, mid.messages, picks.mid.messages,
        acknowledged > 0 and missing == 0 and out_of_order == 0 and appends_again and mid.messages == picks.mid.messages,
    });
}

/// The status of a GET, or null if the node does not answer.
fn getStatus(allocator: Allocator, url: []const u8) ?std.http.Status {
    var http: std.http.Client = .{ .allocator = allocator };
    defer http.deinit();
    var body: [16 * 1024]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&body);
    const result = http.fetch(.{ .method = .GET, .location = .{ .url = url }, .response_writer = &response_writer }) catch return null;
    return result.status;
}

/// With the store's disk full (the caller filled it, and `crash-load` appended until the node
/// refused): the node still answers, says it is not ready, still serves a generated channel
/// whole, and refuses an append rather than acknowledging one it could not store.
fn diskFullCheck(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    const ready_url = try std.fmt.allocPrint(allocator, "{s}/observe/health/ready", .{options.url});
    defer allocator.free(ready_url);
    const ready_status = getStatus(allocator, ready_url);
    const answers = ready_status != null;
    const says_not_ready = ready_status == .service_unavailable;
    const picks = try pick(manifest);
    var read: i64 = -1;
    if (answers) {
        var client = try Client.init(allocator, options.url);
        defer client.deinit();
        const from = ulid_min.encode();
        const to = ulid_max.encode();
        if (readAll(&client, channelQuery(try picks.mid.channelId(), &from, &to, &.{}), std.math.maxInt(u64))) |result| {
            read = @intCast(result.messages);
        } else |_| {}
    }
    const append_refused = probeAppend(allocator, options.url) == null;
    try out.line("{{\"scenario\":\"disk-full-check\",\"node_answers\":{},\"says_not_ready\":{},\"generated_channel\":{d},\"generated_channel_expected\":{d},\"append_refused\":{},\"ok\":{}}}", .{
        answers, says_not_ready, read, picks.mid.messages, append_refused, answers and says_not_ready and read == @as(i64, @intCast(picks.mid.messages)) and append_refused,
    });
}

const CursorHolder = struct {
    allocator: Allocator,
    url: []const u8,
    last_cursor: u64 = 0,
    opened: u64 = 0,

    /// Opens a filtered read of the whole store that matches nothing and fetches from it, again
    /// and again until the node goes away: a fetch is in flight when the node is stopped.
    fn loop(self: *CursorHolder) void {
        var http: std.http.Client = .{ .allocator = self.allocator };
        defer http.deinit();
        const from = ulid_min.encode();
        const to = ulid_max.encode();
        const filters = [_]FilterDto{.{ .kv_filter = .{ .eql = .{ .key = "tracking_id", .value = "no-such-run" } } }};
        while (true) {
            const cursor_id = openRawCursor(self.allocator, &http, self.url, .{
                .query_dto = .{ .query_range = .{ .range_start = &from, .range_end = &to } },
                .filters = &filters,
            }) catch return;
            self.last_cursor = cursor_id;
            self.opened += 1;
            const url = std.fmt.allocPrint(self.allocator, "{s}/api/v1/cursors/0x{x}?batch_size=0x1f", .{ self.url, cursor_id }) catch return;
            defer self.allocator.free(url);
            var body: [64 * 1024]u8 = undefined;
            var response_writer = std.Io.Writer.fixed(&body);
            _ = http.fetch(.{ .method = .GET, .location = .{ .url = url }, .response_writer = &response_writer }) catch return;
        }
    }
};

const cursor_holders = 4;
const idle_cursors = 8;

/// Leaves cursors open for the caller to stop the node under: eight part-read channel cursors
/// nobody is fetching from, and four clients with a long fetch in flight. Returns when the node
/// has gone, and records the cursor ids in `--state` for `cursors-after`.
fn cursorsHold(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    if (options.state.len == 0) return error.StateFileRequired;
    const from = ulid_min.encode();
    const to = ulid_max.encode();
    var idle: [idle_cursors]u64 = undefined;
    {
        var http: std.http.Client = .{ .allocator = allocator };
        defer http.deinit();
        const big = try manifest.channels[0].channelId();
        for (&idle) |*cursor_id| {
            cursor_id.* = try openRawCursor(allocator, &http, options.url, channelQuery(big, &from, &to, &.{}));
            fetchAndHangUp(allocator, options.url, cursor_id.*, 50) catch {};
        }
    }
    var holders: [cursor_holders]CursorHolder = undefined;
    var threads: [cursor_holders]std.Thread = undefined;
    for (&holders, &threads) |*holder, *thread| {
        holder.* = .{ .allocator = allocator, .url = options.url };
        thread.* = try std.Thread.spawn(.{}, CursorHolder.loop, .{holder});
    }
    for (&threads) |*thread| thread.join();

    var file = try std.fs.cwd().createFile(options.state, .{});
    defer file.close();
    var buf: [4096]u8 = undefined;
    var file_writer = file.writer(&buf);
    var opened: u64 = idle_cursors;
    for (idle) |cursor_id| try file_writer.interface.print("{x}\n", .{cursor_id});
    for (&holders) |*holder| {
        opened += holder.opened;
        if (holder.opened > 0) try file_writer.interface.print("{x}\n", .{holder.last_cursor});
    }
    try file_writer.interface.flush();
    try out.line("{{\"scenario\":\"cursors-hold\",\"idle_cursors\":{d},\"fetching_clients\":{d},\"cursors_opened\":{d}}}", .{ idle_cursors, cursor_holders, opened });
}

/// After the node was stopped with cursors open and started again: a fetch from one of the old
/// cursors is refused as a client error, the node appends, and a generated channel reads as it did.
fn cursorsAfter(allocator: Allocator, options: Options, manifest: Manifest, out: *Out) !void {
    if (options.state.len == 0) return error.StateFileRequired;
    const text = try std.fs.cwd().readFileAlloc(allocator, options.state, 1024 * 1024);
    defer allocator.free(text);
    var old: u64 = 0;
    var refused: u64 = 0;
    var lines = std.mem.tokenizeScalar(u8, text, '\n');
    while (lines.next()) |cursor_text| {
        const url = try std.fmt.allocPrint(allocator, "{s}/api/v1/cursors/0x{s}?batch_size=0x1f", .{ options.url, cursor_text });
        defer allocator.free(url);
        old += 1;
        if (getStatus(allocator, url)) |status| {
            if (status.class() == .client_error) refused += 1;
        }
    }
    const appends_again = probeAppend(allocator, options.url) != null;
    const picks = try pick(manifest);
    var client = try Client.init(allocator, options.url);
    defer client.deinit();
    const from = ulid_min.encode();
    const to = ulid_max.encode();
    const mid = try readAll(&client, channelQuery(try picks.mid.channelId(), &from, &to, &.{}), std.math.maxInt(u64));
    try out.line("{{\"scenario\":\"cursors-after\",\"old_cursors\":{d},\"refused\":{d},\"appends_again\":{},\"generated_channel\":{d},\"generated_channel_expected\":{d},\"ok\":{}}}", .{
        old,                                                                                 refused, appends_again, mid.messages, picks.mid.messages,
        old > 0 and refused == old and appends_again and mid.messages == picks.mid.messages,
    });
}

const Stormer = struct {
    allocator: Allocator,
    url: []const u8,
    channel: u128,
    stop: *std.atomic.Value(bool),
    appended: u64 = 0,
    garbled: u64 = 0,
    reconnects: u64 = 0,

    fn connect(self: *Stormer) !std.net.Stream {
        const uri = try std.Uri.parse(self.url);
        var host_buf: [std.Uri.host_name_max]u8 = undefined;
        const host = try uri.getHost(&host_buf);
        return std.net.tcpConnectToHost(self.allocator, host, uri.port orelse 80);
    }

    /// Reads exactly one response and checks every byte the node is known to send: the status
    /// line, a content length, and a body of that length holding a message id.
    fn readResponse(stream: std.net.Stream, buf: []u8) !usize {
        var have: usize = 0;
        var head_end: ?usize = null;
        while (head_end == null) {
            if (have == buf.len) return error.Garbled;
            const got = try std.posix.read(stream.handle, buf[have..]);
            if (got == 0) return error.Closed;
            have += got;
            if (std.mem.indexOf(u8, buf[0..have], "\r\n\r\n")) |at| head_end = at + 4;
        }
        const head = buf[0..head_end.?];
        if (!std.mem.startsWith(u8, head, "HTTP/1.1 201 ")) return error.Garbled;
        const marker = "Content-Length: ";
        const at = std.mem.indexOf(u8, head, marker) orelse return error.Garbled;
        const end = std.mem.indexOfPos(u8, head, at, "\r\n") orelse return error.Garbled;
        const length = std.fmt.parseUnsigned(usize, head[at + marker.len .. end], 10) catch return error.Garbled;
        while (have < head.len + length) {
            const got = try std.posix.read(stream.handle, buf[have..]);
            if (got == 0) return error.Closed;
            have += got;
        }
        // Nothing may follow: the client sent one request
        if (have != head.len + length or length != 26) return error.Garbled;
        _ = common.ulid.decode(buf[head.len..have]) catch return error.Garbled;
        return have;
    }

    fn loop(self: *Stormer) void {
        var request_buf: [512]u8 = undefined;
        const body = std.fmt.bufPrint(request_buf[256..], "{{\"encoded_correlation_id\":null,\"channel\":{d},\"routing\":1,\"kv_entries\":[],\"encoded_msg_body\":\"c3Rvcm0=\"}}", .{self.channel}) catch return;
        const head = std.fmt.bufPrint(request_buf[0..256], "POST /api/v1/messages HTTP/1.1\r\nHost: dymes\r\nContent-Type: application/json\r\nContent-Length: {d}\r\n\r\n", .{body.len}) catch return;
        var response_buf: [4096]u8 = undefined;
        var maybe_stream: ?std.net.Stream = null;
        defer if (maybe_stream) |stream| stream.close();
        while (!self.stop.load(.monotonic)) {
            const stream = maybe_stream orelse blk: {
                const fresh = self.connect() catch {
                    std.Thread.sleep(100 * std.time.ns_per_ms);
                    continue;
                };
                self.reconnects += 1;
                maybe_stream = fresh;
                break :blk fresh;
            };
            const iov = [_]std.posix.iovec_const{ .{ .base = head.ptr, .len = head.len }, .{ .base = body.ptr, .len = body.len } };
            const sent = std.posix.writev(stream.handle, &iov) catch 0;
            const ok = sent == head.len + body.len;
            if (ok) {
                if (readResponse(stream, &response_buf)) |_| {
                    self.appended += 1;
                    continue;
                } else |e| if (e == error.Garbled) {
                    self.garbled += 1;
                    std.debug.print("garbled response on channel {d}: {x}\n", .{ self.channel, response_buf[0..64] });
                }
            }
            stream.close();
            maybe_stream = null;
        }
    }
};

/// Sixteen clients each append one small message at a time over a kept-alive connection, as fast
/// as the node answers, and check every byte of every response.
fn appendStorm(allocator: Allocator, options: Options, out: *Out) !void {
    const clients = 16;
    var stop: std.atomic.Value(bool) = .init(false);
    var stormers: [clients]Stormer = undefined;
    var threads: [clients]std.Thread = undefined;
    for (&stormers, &threads, 0..) |*stormer, *thread, i| {
        stormer.* = .{ .allocator = allocator, .url = options.url, .channel = scratch_channel + 64 + i, .stop = &stop };
        thread.* = try std.Thread.spawn(.{}, Stormer.loop, .{stormer});
    }
    std.Thread.sleep(options.seconds * std.time.ns_per_s);
    stop.store(true, .monotonic);
    for (&threads) |*thread| thread.join();
    var appended: u64 = 0;
    var garbled: u64 = 0;
    var reconnects: u64 = 0;
    for (&stormers) |*stormer| {
        appended += stormer.appended;
        garbled += stormer.garbled;
        reconnects += stormer.reconnects;
    }
    try out.line("{{\"scenario\":\"append-storm\",\"clients\":{d},\"seconds\":{d},\"appended\":{d},\"garbled_responses\":{d},\"connections\":{d},\"ok\":{}}}", .{
        clients, options.seconds, appended, garbled, reconnects, appended > 0 and garbled == 0 and reconnects == clients,
    });
}
