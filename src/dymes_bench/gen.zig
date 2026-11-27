//! Dymes benchmark store generator.
//!
//! Writes a store through the message store library (not over HTTP), so a store of production size
//! takes minutes. The same seed, message count and end time give the same store.
//!
//! The shape follows a feed-ingestion workload: a few channels hold most of the messages and many
//! hold dozens; messages arrive in runs (a marker, records correlated to the marker, a closing
//! message) that share a `tracking_id` header; runs of different channels interleave in bursts.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const Health = common.health.Health;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const FrameAllocator = dymes_msg.FrameAllocator;

const msg_store = @import("dymes_msg_store");
const AppendStore = msg_store.AppendStore;
const MessageStoreOptions = msg_store.MessageStoreOptions;

pub const Options = struct {
    /// Store directory (created if missing; must hold no store)
    dir: []const u8,
    /// Manifest file: what was written, for the scenarios to query and check
    manifest: []const u8,
    /// Messages to write
    messages: u64 = 1_000_000,
    /// Feed channels (one more, the sparse channel, is added)
    channels: usize = 160,
    seed: u64 = 20261127,
    /// Time span the message ids cover
    days: u64 = 90,
    /// Time of the newest message, ms since the epoch (default: one minute ago)
    end_ms: ?u64 = null,
};

/// Channel domain of generated feed channels (top byte of the channel id).
pub const channel_domain: u128 = 0x05;
/// Channel index of the sparse channel: about one message in `sparse_every` bursts.
pub const sparse_every: u64 = 20_000;

/// Routing domains (top 4 bits of the low 64 bits).
pub const routing_domain_feeds: u128 = 0x9;
pub const routing_domain_sparse: u128 = 0x3;
/// Mask and comparand selecting the sparse routing domain (a narrowing routing filter).
pub const routing_domain_mask: u128 = 0xF << 60;
pub const routing_sparse_cmp: u128 = routing_domain_sparse << 60;

const class_marker: u128 = 0xA0;
const class_record: u128 = 0xA1;
const class_closed: u128 = 0xA2;

const max_samples = 3;

const RunSample = struct {
    tracking_id: [36]u8 = undefined,
    first_id: Ulid = .{ .time = 0, .rand = 0 },
    last_id: Ulid = .{ .time = 0, .rand = 0 },
    messages: u64 = 0,
};

const ChannelState = struct {
    id: u128,
    routing_base: u128,
    /// Records per run: uniform in [min, max]
    run_min: u32,
    run_max: u32,

    messages: u64 = 0,
    first_id: Ulid = .{ .time = 0, .rand = 0 },
    last_id: Ulid = .{ .time = 0, .rand = 0 },
    runs: u64 = 0,

    // Current run
    in_run: bool = false,
    records_left: u32 = 0,
    marker_id: Ulid = .{ .time = 0, .rand = 0 },
    current: RunSample = .{},

    // Completed runs kept for the scenarios: the first, a random one, the latest
    samples: [max_samples]RunSample = [_]RunSample{.{}} ** max_samples,
    num_samples: usize = 0,
};

pub fn channelId(index: usize) u128 {
    return (channel_domain << 120) | @as(u128, index + 1);
}

/// Share of the bursts each channel gets. A few channels take most; the tail gets almost none.
fn buildWeights(allocator: Allocator, channels: usize) ![]f64 {
    const weights = try allocator.alloc(f64, channels);
    for (weights, 0..) |*w, i| {
        w.* = if (i == 0) 0.33 else if (i < 3) 0.08 else if (i < 8) 0.04 else if (i < 23) 0.015 else if (i < 63) 0.002 else blk: {
            // Geometric tail from 1e-4 down to 5e-7
            const tail_len: f64 = @floatFromInt(@max(channels - 63, 2) - 1);
            const pos: f64 = @floatFromInt(i - 63);
            break :blk 1.0e-4 * std.math.pow(f64, 5.0e-3, pos / tail_len);
        };
    }
    var total: f64 = 0;
    for (weights) |w| total += w;
    var cumulative: f64 = 0;
    for (weights) |*w| {
        cumulative += w.* / total;
        w.* = cumulative;
    }
    weights[channels - 1] = 1.0;
    return weights;
}

fn pickChannel(cumulative: []const f64, r: f64) usize {
    var low: usize = 0;
    var high: usize = cumulative.len - 1;
    while (low < high) {
        const mid = (low + high) / 2;
        if (cumulative[mid] < r) low = mid + 1 else high = mid;
    }
    return low;
}

fn fillTrackingId(random: std.Random, out: *[36]u8) void {
    const hex = "0123456789abcdef";
    for (out, 0..) |*c, i| {
        c.* = if (i == 8 or i == 13 or i == 18 or i == 23) '-' else hex[random.uintLessThan(usize, 16)];
    }
}

const Generator = struct {
    allocator: Allocator,
    store: *AppendStore,
    prng: std.Random.DefaultPrng,
    frame: []u8,
    filler: []u8,
    headers: std.StringArrayHashMap([]const u8),
    start_ms: u64,
    span_ms: u64,
    total: u64,
    written: u64 = 0,
    bytes: u64 = 0,

    fn nextId(self: *Generator) Ulid {
        const time_ms = self.start_ms + @as(u64, @intCast((@as(u128, self.written) * self.span_ms) / self.total));
        return .{ .time = @intCast(time_ms), .rand = @intCast(self.written + 1) };
    }

    fn write(self: *Generator, ch: *ChannelState, channel_index: usize, class: u128, routing_domain: u128, correlation: ?Ulid, event_type: []const u8) !Ulid {
        const random = self.prng.random();
        const id = self.nextId();

        var hash_buf: [64]u8 = undefined;
        const hex = "0123456789abcdef";
        for (&hash_buf) |*c| c.* = hex[random.uintLessThan(usize, 16)];
        var dataset_buf: [24]u8 = undefined;
        const dataset = std.fmt.bufPrint(&dataset_buf, "feed.ds{d:0>3}", .{channel_index}) catch unreachable;
        var key_buf: [48]u8 = undefined;
        const natural_key = std.fmt.bufPrint(&key_buf, "{d}:{d}", .{ ch.runs, ch.messages }) catch unreachable;
        var url_buf: [160]u8 = undefined;
        const source_url = std.fmt.bufPrint(&url_buf, "https://data.example.org/api/v1/datasets/DS{d:0>3}/stream?from={d}&to={d}&format=json", .{ channel_index, id.time, id.time + 1_800_000 }) catch unreachable;
        var ws_buf: [24]u8 = undefined;
        const window_start = std.fmt.bufPrint(&ws_buf, "{d}", .{id.time}) catch unreachable;
        var we_buf: [24]u8 = undefined;
        const window_end = std.fmt.bufPrint(&we_buf, "{d}", .{id.time + 1_800_000}) catch unreachable;
        var idem_buf: [80]u8 = undefined;
        const idempotency_key = std.fmt.bufPrint(&idem_buf, "{s}:{d}", .{ ch.current.tracking_id, ch.current.messages }) catch unreachable;

        self.headers.clearRetainingCapacity();
        try self.headers.put("event_type", event_type);
        try self.headers.put("schema", "native/1");
        try self.headers.put("dataset", dataset);
        try self.headers.put("tracking_id", &ch.current.tracking_id);
        try self.headers.put("natural_key", natural_key);
        try self.headers.put("payload_hash", &hash_buf);
        try self.headers.put("window_start", window_start);
        try self.headers.put("window_end", window_end);
        try self.headers.put("source_url", source_url);
        try self.headers.put("fetched_at", window_end);
        try self.headers.put("idempotency_key", idempotency_key);
        try self.headers.put("attempt", "1");

        // Bodies of 200 to 1400 bytes: with the headers, frames average about 1.3 KiB
        const body_len = if (class == class_record) 200 + random.uintLessThan(usize, 1200) else 96;
        const body_at = random.uintLessThan(usize, self.filler.len - body_len);
        const body = self.filler[body_at .. body_at + body_len];

        const routing: u128 = (ch.routing_base & ~routing_domain_mask & ~(@as(u128, 0xFF) << 40)) | (routing_domain << 60) | (class << 40);
        const msg = try Message.initOverlay(self.frame, 0, id, ch.id, routing, body, .{
            .correlation_id = correlation orelse dymes_msg.constants.NIL_CORRELATION_ID,
            .transient_kv_headers = self.headers,
        });
        _ = try self.store.store(&msg);

        self.written += 1;
        self.bytes += msg.frame_header.frame_size;
        if (ch.messages == 0) ch.first_id = id;
        ch.messages += 1;
        ch.last_id = id;
        if (ch.current.messages == 0) ch.current.first_id = id;
        ch.current.messages += 1;
        ch.current.last_id = id;
        return id;
    }

    /// Writes up to `burst` messages of the channel's current run, starting a run if none is open.
    fn burst(self: *Generator, ch: *ChannelState, channel_index: usize, burst_len: u32) !void {
        const random = self.prng.random();
        var left = burst_len;
        while (left > 0 and self.written < self.total) : (left -= 1) {
            if (!ch.in_run) {
                ch.current = .{};
                fillTrackingId(random, &ch.current.tracking_id);
                ch.records_left = ch.run_min + random.uintLessThan(u32, ch.run_max - ch.run_min + 1);
                ch.marker_id = try self.write(ch, channel_index, class_marker, routing_domain_feeds, null, "job_marker");
                ch.in_run = true;
            } else if (ch.records_left > 0) {
                _ = try self.write(ch, channel_index, class_record, routing_domain_feeds, ch.marker_id, "record");
                ch.records_left -= 1;
            } else {
                _ = try self.write(ch, channel_index, class_closed, routing_domain_feeds, ch.marker_id, "window_closed");
                ch.in_run = false;
                ch.runs += 1;
                // Keep the first run, then a random one, and always the latest
                if (ch.num_samples < max_samples) {
                    ch.samples[ch.num_samples] = ch.current;
                    ch.num_samples += 1;
                } else {
                    if (random.uintLessThan(u64, ch.runs) < 2) ch.samples[1] = ch.samples[2];
                    ch.samples[2] = ch.current;
                }
                return; // a run's end ends the burst
            }
        }
    }
};

pub fn run(allocator: Allocator, options: Options) !void {
    var stderr_buf: [512]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_writer.interface;

    if (options.channels < 64) return error.TooFewChannels;

    std.fs.cwd().makePath(options.dir) catch {};
    var dir = try std.fs.cwd().openDir(options.dir, .{ .iterate = true });
    defer dir.close();
    {
        var it = dir.iterate();
        if (try it.next()) |_| {
            try stderr.print("gen: {s} is not empty; refusing to write into it\n", .{options.dir});
            try stderr.flush();
            return error.DirectoryNotEmpty;
        }
    }

    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();
    var health = Health.init(allocator);
    defer health.deinit();
    var store_options = try MessageStoreOptions.init(allocator, dir, options.dir, &health);
    defer store_options.deinit(allocator);
    const store = try AppendStore.open(allocator, frame_allocator, store_options);
    defer store.close();

    const end_ms: u64 = options.end_ms orelse @intCast(std.time.milliTimestamp() - std.time.ms_per_min);
    const span_ms: u64 = options.days * std.time.ms_per_day;

    const cumulative = try buildWeights(allocator, options.channels);
    defer allocator.free(cumulative);

    // Feed channels, then the sparse channel
    const states = try allocator.alloc(ChannelState, options.channels + 1);
    defer allocator.free(states);
    for (states, 0..) |*st, i| {
        const run_min: u32, const run_max: u32 = if (i < 8) .{ 500, 5000 } else if (i < 63) .{ 20, 500 } else .{ 1, 20 };
        st.* = .{
            .id = channelId(i),
            // Low 64 bits: domain, class, family, dataset; high 64 bits: tenant, deployment
            .routing_base = (@as(u128, 1) << 96) | (@as(u128, 0x202) << 64) | (@as(u128, i % 7) << 16) | @as(u128, i),
            .run_min = run_min,
            .run_max = run_max,
        };
    }
    const sparse = &states[options.channels];
    sparse.run_min = 0;
    sparse.run_max = 0;

    var gen: Generator = .{
        .allocator = allocator,
        .store = store,
        .prng = std.Random.DefaultPrng.init(options.seed),
        .frame = try allocator.alloc(u8, 64 * 1024),
        .filler = try allocator.alloc(u8, 1024 * 1024),
        .headers = std.StringArrayHashMap([]const u8).init(allocator),
        .start_ms = end_ms - span_ms,
        .span_ms = span_ms,
        .total = options.messages,
    };
    defer allocator.free(gen.frame);
    defer allocator.free(gen.filler);
    defer gen.headers.deinit();
    const random = gen.prng.random();
    const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 {}[]\":,.-";
    for (gen.filler) |*c| c.* = alphabet[random.uintLessThan(usize, alphabet.len)];

    var timer = try std.time.Timer.start();
    var next_report: u64 = 1_000_000;
    while (gen.written < gen.total) {
        if (random.uintLessThan(u64, sparse_every) == 0) {
            // One message in the sparse routing domain, alone on its channel
            sparse.current = .{};
            fillTrackingId(random, &sparse.current.tracking_id);
            _ = try gen.write(sparse, options.channels, class_record, routing_domain_sparse, null, "record");
            sparse.runs += 1;
            continue;
        }
        const index = pickChannel(cumulative, random.float(f64));
        try gen.burst(&states[index], index, 1 + random.uintLessThan(u32, 64));
        if (gen.written >= next_report) {
            next_report += 1_000_000;
            try stderr.print("gen: {d} of {d} messages, {d} MiB, {d} s\n", .{ gen.written, gen.total, gen.bytes / (1024 * 1024), timer.read() / std.time.ns_per_s });
            try stderr.flush();
        }
    }
    try store.sync();

    try writeManifest(options, states, &gen, end_ms);
    try stderr.print("gen: done: {d} messages, {d} MiB of frames, {d} s\n", .{ gen.written, gen.bytes / (1024 * 1024), timer.read() / std.time.ns_per_s });
    try stderr.flush();
}

fn writeManifest(options: Options, states: []const ChannelState, gen: *const Generator, end_ms: u64) !void {
    var file = try std.fs.cwd().createFile(options.manifest, .{});
    defer file.close();
    var buf: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(&buf);
    const w = &file_writer.interface;

    try w.print("{{\n  \"seed\": {d},\n  \"messages\": {d},\n  \"frame_bytes\": {d},\n  \"start_ms\": {d},\n  \"end_ms\": {d},\n  \"feed_channels\": {d},\n  \"sparse_channel\": {d},\n  \"channels\": [\n", .{
        options.seed, gen.written, gen.bytes, gen.start_ms, end_ms, options.channels, options.channels,
    });
    for (states, 0..) |st, i| {
        try w.print("    {{\"index\": {d}, \"id\": \"{d}\", \"messages\": {d}, \"runs\": {d}, \"first_id\": \"{s}\", \"last_id\": \"{s}\", \"samples\": [", .{
            i, st.id, st.messages, st.runs, st.first_id.encode(), st.last_id.encode(),
        });
        for (st.samples[0..st.num_samples], 0..) |sample, s| {
            try w.print("{s}{{\"tracking_id\": \"{s}\", \"messages\": {d}, \"first_id\": \"{s}\", \"last_id\": \"{s}\"}}", .{
                if (s == 0) "" else ", ", sample.tracking_id, sample.messages, sample.first_id.encode(), sample.last_id.encode(),
            });
        }
        try w.print("]}}{s}\n", .{if (i + 1 == states.len) "" else ","});
    }
    try w.print("  ]\n}}\n", .{});
    try w.flush();
}
