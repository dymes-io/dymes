//! Dymes Client.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;
const Uri = std.Uri;
const http = std.http;

const common = @import("dymes_common");
const ulid = common.ulid;
const Ulid = ulid.Ulid;
const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const AllocationError = errors.AllocationError;
const NetworkError = errors.NetworkError;
const ClientError = errors.ClientError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const Logger = logging.Logger;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const MessageBuilder = dymes_msg.MessageBuilder;
const CreationRequest = dymes_msg.CreationRequest;
const CreationRequestBuilder = dymes_msg.CreationRequestBuider;
const ImportRequest = dymes_msg.ImportRequest;
const ImportRequestBuilder = dymes_msg.ImportRequestBuilder;
const FrameAllocator = dymes_msg.FrameAllocator;

const constants = @import("constants.zig");
const limits = @import("limits.zig");

const queries = @import("queries.zig");

const Filter = queries.Filter;
const FilterDto = queries.FilterDto;
const ClientQueryDto = queries.ClientQueryDto;

const component_name = "client.Client";

const marshal_ulids: bool = false;

const short_response_limit: usize = 1024; // 1KiB

const Self = @This();

const OpenError = AllocationError || UsageError || NetworkError;

allocator: std.mem.Allocator,
fai: *FrameAllocator,
frame_allocator: std.mem.Allocator,
logger: *Logger,
append_url: []const u8,
append_uri: Uri, // Note: URI slices point into URL
import_url: []const u8,
import_uri: Uri,
query_single_url_base: []const u8,
query_cursor_url_base: []const u8,
http_client: std.http.Client,
short_result_buf: []u8,
msg_result_buf: []u8,
json_write_buf: []u8,
open_cursors: std.hash_map.AutoHashMap(u64, *ForwardCursor),

pub fn init(allocator: std.mem.Allocator, base_url: []const u8) OpenError!Self {
    var logger = logging.logger(component_name);
    const append_url = std.mem.concat(allocator, u8, &.{ base_url, constants.http_path_prefix ++ "/messages" }) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(append_url);
    const append_uri = Uri.parse(append_url) catch |e| {
        logger.err()
            .msg("Failed to parse append URL")
            .err(e)
            .str("append_url", append_url)
            .log();
        return UsageError.IllegalArgument;
    };
    const import_url = std.mem.concat(allocator, u8, &.{ base_url, constants.http_path_prefix ++ "/import" }) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(import_url);
    const import_uri = Uri.parse(import_url) catch |e| {
        logger.err()
            .msg("Failed to parse import URL")
            .err(e)
            .str("import_url", import_url)
            .log();
        return UsageError.IllegalArgument;
    };

    const query_single_url_base = std.mem.concat(allocator, u8, &.{ append_url, "/" }) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(query_single_url_base);

    const query_cursor_url_base = std.mem.concat(allocator, u8, &.{ base_url, constants.http_path_prefix ++ "/cursors" }) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(query_cursor_url_base);

    var frame_alloc_impl = allocator.create(FrameAllocator) catch return AllocationError.OutOfMemory;
    frame_alloc_impl.* = FrameAllocator.init(allocator);
    const frame_allocator = frame_alloc_impl.allocator();

    const short_result_buf = allocator.alloc(u8, limits.short_result_buffer_size) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(short_result_buf);

    const msg_result_buf = allocator.alloc(u8, limits.msg_result_buffer_size) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(msg_result_buf);

    const json_write_buf = allocator.alloc(u8, limits.json_write_buffer_size) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(json_write_buf);

    var open_cursors = std.hash_map.AutoHashMap(u64, *ForwardCursor).init(allocator);
    errdefer open_cursors.deinit();

    return .{
        .allocator = allocator,
        .fai = frame_alloc_impl,
        .frame_allocator = frame_allocator,
        .logger = logger,
        .append_url = append_url,
        .append_uri = append_uri,
        .import_url = import_url,
        .import_uri = import_uri,
        .query_single_url_base = query_single_url_base,
        .query_cursor_url_base = query_cursor_url_base,
        .http_client = .{ .allocator = allocator },
        .short_result_buf = short_result_buf,
        .msg_result_buf = msg_result_buf,
        .json_write_buf = json_write_buf,
        .open_cursors = open_cursors,
    };
}

pub fn deinit(self: *Self) void {
    defer self.http_client.deinit();
    defer self.allocator.free(self.append_url);
    defer self.allocator.free(self.import_url);
    defer self.allocator.free(self.query_single_url_base);
    defer self.allocator.free(self.query_cursor_url_base);
    defer self.allocator.free(self.json_write_buf);
    defer self.allocator.free(self.short_result_buf);
    defer self.allocator.free(self.msg_result_buf);
    defer self.allocator.destroy(self.fai);
    defer self.open_cursors.deinit();
    var it = self.open_cursors.iterator();
    while (it.next()) |_entry| {
        _entry.value_ptr.*.deinit();
    }
}

const MessageCreationError = AllocationError || UsageError || NetworkError || ClientError;

/// Appends a message
pub fn appendMessage(self: *Self, msg_body: []const u8, channel: u128, routing: u128, kv_map: ?std.array_hash_map.StringArrayHashMap([]const u8), correlation_id: ?Ulid) MessageCreationError!Ulid {
    var req_bld = try CreationRequestBuilder.init(self.allocator);
    defer req_bld.deinit();

    try req_bld.withChannel(channel);
    try req_bld.withRouting(routing);
    try req_bld.withCorrelationId(correlation_id);
    if (kv_map) |_kv_map| {
        var kv_it = _kv_map.iterator();
        while (kv_it.next()) |kv| {
            try req_bld.withKvPair(kv.key_ptr.*, kv.value_ptr.*);
        }
    }
    try req_bld.withBody(msg_body);

    var creation_request = try req_bld.build();
    defer creation_request.deinit(self.allocator);

    var req_json = std.array_list.Managed(u8).init(self.allocator);
    defer req_json.deinit();

    var json_writer = std.Io.Writer.fixed(self.json_write_buf);
    std.json.Stringify.value(creation_request, .{ .whitespace = .minified }, &json_writer) catch return MessageCreationError.LimitReached;

    var response_writer = std.Io.Writer.fixed(self.short_result_buf);
    const fetch_options: std.http.Client.FetchOptions = .{
        .method = .POST,
        .location = .{ .uri = self.append_uri },
        .payload = json_writer.buffered(),
        .response_writer = &response_writer,
    };

    const result = self.http_client.fetch(fetch_options) catch |e| {
        self.logger.err()
            .msg("Failed to append message")
            .err(e)
            .intx("channel", channel)
            .intx("routing", routing)
            .log();

        return switch (e) {
            error.StreamTooLong => UsageError.IllegalArgument,
            error.WriteFailed => NetworkError.SocketFailure,
            error.UnsupportedCompressionMethod => UsageError.UnsupportedOperation,
            else => ClientError.ClientFailure,
        };
    };

    return switch (result.status) {
        .ok, .created => happy: {
            const response = response_writer.buffered();
            const appended = ulid.decode(response) catch |e| {
                self.logger.err()
                    .msg("Unable to decode server response")
                    .err(e)
                    .str("response", response)
                    .log();
                return ClientError.ClientFailure;
            };
            break :happy appended;
        },
        .bad_request => unhappy: {
            self.logger.err()
                .msg("Server rejected our request")
                .int("status", result.status)
                .str("result", response_writer.buffered())
                .log();
            break :unhappy ClientError.ClientFailure;
        },
        else => {
            return ClientError.ClientFailure;
        },
    };
}

// Imports a message
pub fn importMessage(self: *Self, msg_body: []const u8, channel: u128, routing: u128, kv_map: ?std.array_hash_map.StringArrayHashMap([]const u8), correlation_id: ?Ulid, id: Ulid) MessageCreationError!Ulid {
    var req_bld = try ImportRequestBuilder.init(self.allocator);
    defer req_bld.deinit();

    try req_bld.withChannel(channel);
    try req_bld.withRouting(routing);
    try req_bld.withCorrelationId(correlation_id);
    try req_bld.withId(id);
    if (kv_map) |_kv_map| {
        var kv_it = _kv_map.iterator();
        while (kv_it.next()) |kv| {
            try req_bld.withKvPair(kv.key_ptr.*, kv.value_ptr.*);
        }
    }
    try req_bld.withBody(msg_body);

    var import_request = try req_bld.build();
    defer import_request.deinit(self.allocator);

    var req_json = std.array_list.Managed(u8).init(self.allocator);
    defer req_json.deinit();

    var json_writer = std.Io.Writer.fixed(self.json_write_buf);
    std.json.Stringify.value(import_request, .{ .whitespace = .minified }, &json_writer) catch return MessageCreationError.LimitReached;

    var response_writer = std.Io.Writer.fixed(self.short_result_buf);
    const fetch_options: std.http.Client.FetchOptions = .{
        .method = .POST,
        .location = .{ .uri = self.import_uri },
        .payload = json_writer.buffered(),
        .response_writer = &response_writer,
    };

    const result = self.http_client.fetch(fetch_options) catch |e| {
        self.logger.err()
            .msg("Failed to import message")
            .err(e)
            .intx("channel", channel)
            .intx("routing", routing)
            .log();

        return switch (e) {
            error.StreamTooLong => UsageError.IllegalArgument,
            error.WriteFailed => NetworkError.SocketFailure,
            error.UnsupportedCompressionMethod => UsageError.UnsupportedOperation,
            else => ClientError.ClientFailure,
        };
    };

    return switch (result.status) {
        .ok, .created => happy: {
            const response = response_writer.buffered();
            const appended = ulid.decode(response) catch |e| {
                self.logger.err()
                    .msg("Unable to decode server response")
                    .err(e)
                    .str("response", response)
                    .log();
                return ClientError.ClientFailure;
            };
            break :happy appended;
        },
        .bad_request => unhappy: {
            self.logger.err()
                .msg("Server rejected our request")
                .int("status", result.status)
                .str("result", response_writer.buffered())
                .log();
            break :unhappy ClientError.ClientFailure;
        },
        else => {
            return ClientError.ClientFailure;
        },
    };
}

const MessageQueryError = AllocationError || UsageError || NetworkError || ClientError;

/// Queries a single message
///
/// It's the caller's responsibility to `deinit()` the returned message.
pub fn queryMessage(self: *Self, msg_id: Ulid) MessageQueryError!?Message {
    const query_url = std.mem.concat(self.allocator, u8, &.{ self.query_single_url_base, &msg_id.encode() }) catch return AllocationError.OutOfMemory;
    defer self.allocator.free(query_url);

    var response_writer = std.Io.Writer.fixed(self.msg_result_buf);
    const fetch_options: std.http.Client.FetchOptions = .{
        .method = .GET,
        .location = .{ .url = query_url },
        .max_append_size = dymes_msg.limits.max_frame_size,
        .response_writer = &response_writer,
    };

    const result = self.http_client.fetch(fetch_options) catch |e| {
        self.logger.err()
            .msg("Failed to query message")
            .err(e)
            .ulid("msg_id", msg_id)
            .log();

        return switch (e) {
            error.StreamTooLong => UsageError.IllegalArgument,
            error.WriteFailed => NetworkError.SocketFailure,
            error.UnsupportedCompressionMethod => UsageError.UnsupportedOperation,
            else => ClientError.ClientFailure,
        };
    };

    return switch (result.status) {
        .ok => happy: {
            const response = response_writer.buffered();
            if (response.len < dymes_msg.constants.message_frame_header_size) {
                self.logger.err()
                    .msg("Response truncated")
                    .err(UsageError.IllegalArgument)
                    .log();
                return UsageError.IllegalArgument;
            }

            if (marshal_ulids) {
                const id_offset = @offsetOf(dymes_msg.FrameHeader, "id");
                const correlation_id_offset = @offsetOf(dymes_msg.FrameHeader, "correlation_id");

                const marshalled_id = ulid.fromBytes(response[id_offset..][0..@sizeOf(Ulid)]) catch return ClientError.ClientFailure;
                const marshalled_correlation_id = ulid.fromBytes(response[correlation_id_offset..][0..@sizeOf(Ulid)]) catch return ClientError.ClientFailure;
                const message_id = dymes_msg.marshalling.unmarshalUlid(marshalled_id);
                const correlation_id = dymes_msg.marshalling.unmarshalUlid(marshalled_correlation_id);

                @memcpy(response[id_offset .. id_offset + @sizeOf(Ulid)], &message_id.bytes());
                @memcpy(response[correlation_id_offset .. correlation_id_offset + @sizeOf(Ulid)], &correlation_id.bytes());
            }

            const result_msg = Message.overlay(response);
            result_msg.verifyChecksums() catch |e| {
                self.logger.err()
                    .msg("Message was mangled en-route")
                    .err(e)
                    .log();
                return UsageError.IllegalArgument;
            };
            break :happy result_msg;
        },
        .not_found => return null,
        .bad_request => unhappy: {
            self.logger.err()
                .msg("Server rejected our request")
                .int("status", result.status)
                .str("result", self.msg_result_list.items)
                .log();
            break :unhappy ClientError.ClientFailure;
        },
        else => {
            self.logger.err()
                .msg("Unexpected status code")
                .int("status", result.status)
                .log();
            return ClientError.ClientFailure;
        },
    };
}

pub const OpenCursorOptions = struct {
    batch_size: usize = limits.default_forward_cursor_batch_size,
};

/// Opens a cursor, which involves executing the query and opening a cursor for the query results
///
/// It's the caller's responsibility to `close()` the returned cursor.
pub fn openCursor(self: *Self, query: ClientQueryDto, options: OpenCursorOptions) MessageQueryError!?*ForwardCursor {
    if (options.batch_size >= limits.max_forward_cursor_batch_size) {
        self.logger.err()
            .msg("Cursor batch fetch size exceeds limit")
            .int("batch_size", options.batch_size)
            .int("limit", limits.max_forward_cursor_batch_size)
            .log();
        return AllocationError.LimitReached;
    }

    // Open cursor
    const cursor_id: u64 = try self.cursorQuery(query);
    var cursor = try ForwardCursor.open(self.allocator, cursor_id, self, options.batch_size);
    errdefer cursor.deinit();

    // Remember the cursor for later cleanup
    self.open_cursors.put(cursor_id, cursor) catch return AllocationError.OutOfMemory;

    return cursor;
}

/// Performs a cursor query, returning the cursor id
fn cursorQuery(self: *Self, query: ClientQueryDto) MessageQueryError!u64 {
    self.logger.fine()
        .msg("Performing cursor query")
        .any("query", query)
        .log();

    const json_query = std.json.Stringify.valueAlloc(self.allocator, query, .{ .whitespace = .minified }) catch return AllocationError.OutOfMemory;
    defer self.allocator.free(json_query);

    var response_writer = std.Io.Writer.fixed(self.short_result_buf);

    const fetch_options: std.http.Client.FetchOptions = .{
        .method = .POST,
        .location = .{ .url = self.query_cursor_url_base },
        .payload = json_query,
        .response_writer = &response_writer,
    };

    const result = self.http_client.fetch(fetch_options) catch |e| {
        self.logger.err()
            .msg("Failed to perform cursor query")
            .err(e)
            .obj("query", query)
            .log();

        return switch (e) {
            error.StreamTooLong => UsageError.IllegalArgument,
            error.WriteFailed => NetworkError.SocketFailure,
            error.UnsupportedCompressionMethod => UsageError.UnsupportedOperation,
            else => ClientError.ClientFailure,
        };
    };

    return switch (result.status) {
        .created => happy: {
            const response = response_writer.buffered();
            const cursor_id = std.fmt.parseInt(u64, response, 16) catch |e| {
                self.logger.err()
                    .msg("Failed to parse cursor identifier")
                    .err(e)
                    .str("buffer", response)
                    .log();
                return UsageError.IllegalConversion;
            };
            break :happy cursor_id;
        },
        .bad_request => unhappy: {
            self.logger.err()
                .msg("Server rejected our request")
                .int("status", result.status)
                .str("result", response_writer.buffered())
                .log();
            break :unhappy ClientError.ClientFailure;
        },
        else => {
            self.logger.err()
                .msg("Unexpected status code")
                .int("status", result.status)
                .log();
            return ClientError.ClientFailure;
        },
    };
}

/// Lets the client 'forget' a cursor, so it won't be cleaned up as part of client teardown
fn forgetCursor(self: *Self, cursor: *ForwardCursor) void {
    if (!self.open_cursors.remove(cursor.cursor_id)) {
        self.logger.warn()
            .msg("Cursor already forgotten")
            .intx("cursor_id", cursor.cursor_id)
            .log();
    }
}

fn buildRangeQueryBody(self: *Self, start: Ulid, end: Ulid, filters: []const Filter) ClientError![]const u8 {
    const range_query: ClientQueryDto = .{
        .ranged = .{
            .start = start.encode(),
            .end = end.encode(),
        },
        .filters = filters,
    };

    var req_json = std.array_list.Managed(u8).init(self.allocator);
    defer req_json.deinit();

    var json_writer = std.Io.Writer.fixed(self.json_write_buf);
    std.json.Stringify.value(range_query, .{ .whitespace = .minified }, &json_writer) catch |e| {
        self.logger.err()
            .msg("Failed to build body of ranged client query")
            .err(e)
            .log();
        return ClientError.ClientFailure;
    };

    return self.allocator.dupe(u8, json_writer.buffered()) catch return AllocationError.OutOfMemory;
}

fn buildMultiMsgQueryBody(self: *Self, msg_ids: []const Ulid, filters: []const Filter) error{ ClientError, AllocationError }![]const u8 {
    var encoded_ids = self.allocator.alloc([]u8, msg_ids.len) catch return AllocationError.OutOfMemory;
    defer self.allocator.free(encoded_ids);
    defer for (encoded_ids) |enc_msg_id_| {
        self.allocator.free(enc_msg_id_);
    };
    for (msg_ids, 0..) |msg_id_, idx| {
        encoded_ids[idx] = msg_id_.encodeAlloc(self.allocator) catch return AllocationError.OutOfMemory;
    }
    const multi_query: ClientQueryDto = .{
        .multi_msg = .{
            .msg_ids = encoded_ids,
        },
        .filters = filters,
    };

    var req_json = std.array_list.Managed(u8).init(self.allocator);
    defer req_json.deinit();

    var json_writer = std.Io.Writer.fixed(self.json_write_buf);
    std.json.Stringify.value(multi_query, .{ .whitespace = .minified }, &json_writer) catch |e| {
        self.logger.err()
            .msg("Failed to build body of multi-message client query")
            .err(e)
            .log();
        return ClientError.ClientFailure;
    };

    return self.allocator.dupe(u8, json_writer.buffered()) catch return AllocationError.OutOfMemory;
}

const Client = @This();

pub const ForwardCursor = struct {
    const fwd_cursor_component_name = "client.ForwardCursor";

    allocator: std.mem.Allocator,
    logger: *Logger,
    client: *Client,
    cursor_id: u64,
    query_cursor_fetch_url: []const u8,
    query_cursor_close_url: []const u8,
    batch: []Message,
    num_results: usize = 0,
    idx_result: usize = 0,
    final_batch: bool = false,

    /// Initializes the forward cursor.
    ///
    /// Caller must call `close()` to release resources.
    ///
    /// Note that the client should not be closed while the cursor is in use - Bad Things (tm) will happen.
    fn open(allocator: std.mem.Allocator, cursor_id: u64, client: *Client, batch_size: usize) AllocationError!*ForwardCursor {
        assert(batch_size <= limits.max_forward_cursor_batch_size);

        const fetch_url = std.fmt.allocPrint(allocator, "{s}/0x{x}?batch_size=0x{x}", .{
            client.query_cursor_url_base,
            cursor_id,
            batch_size,
        }) catch return AllocationError.OutOfMemory;
        errdefer allocator.free(fetch_url);

        const close_url = std.fmt.allocPrint(allocator, "{s}/0x{x}", .{
            client.query_cursor_url_base,
            cursor_id,
        }) catch return AllocationError.OutOfMemory;
        errdefer allocator.free(close_url);

        const batch = allocator.alloc(Message, batch_size) catch return AllocationError.OutOfMemory;
        errdefer allocator.free(batch);
        const new_self = allocator.create(ForwardCursor) catch return AllocationError.OutOfMemory;
        errdefer allocator.destroy(new_self);

        var logger = logging.logger(fwd_cursor_component_name);

        new_self.* = .{
            .allocator = allocator,
            .logger = logger,
            .client = client,
            .cursor_id = cursor_id,
            .query_cursor_fetch_url = fetch_url,
            .query_cursor_close_url = close_url,
            .batch = batch,
        };

        logger.fine()
            .msg("Server-side cursor opened")
            .intx("cursor_id", cursor_id)
            .log();

        return new_self;
    }

    pub fn close(self: *ForwardCursor) void {
        defer self.deinit();
        defer self.client.forgetCursor(self);
        defer self.closeServerCursor();
        self.logger.fine()
            .msg("Server-side cursor closed")
            .intx("cursor_id", self.cursor_id)
            .log();
    }

    fn deinit(self: *ForwardCursor) void {
        defer self.allocator.destroy(self);
        defer self.allocator.free(self.query_cursor_fetch_url);
        defer self.allocator.free(self.query_cursor_close_url);
        defer self.allocator.free(self.batch);
    }

    fn closeServerCursor(self: *ForwardCursor) void {
        var response_writer = std.Io.Writer.fixed(self.client.short_result_buf);
        const fetch_options: std.http.Client.FetchOptions = .{
            .method = .DELETE,
            .location = .{ .url = self.query_cursor_close_url },
            .response_writer = &response_writer,
        };

        const result = self.client.http_client.fetch(fetch_options) catch |e| {
            self.logger.warn()
                .msg("Failed to close server-side cursor")
                .err(e)
                .intx("cursor_id", self.cursor_id)
                .log();
            return;
        };

        switch (result.status) {
            .ok => {},
            .bad_request => {
                self.logger.warn()
                    .msg("Server rejected our request to close server-side cursor")
                    .int("status", result.status)
                    .intx("cursor_id", self.cursor_id)
                    .log();
            },
            else => {
                self.logger.warn()
                    .msg("Unexpected status code attempting to close server-side cursor")
                    .int("status", result.status)
                    .intx("cursor_id", self.cursor_id)
                    .log();
            },
        }
    }

    const FetchError = AllocationError || UsageError || NetworkError || ClientError;

    pub fn next(self: *ForwardCursor) FetchError!?Message {
        if (self.idx_result >= self.num_results) {
            if (self.final_batch) {
                return null;
            }
            // Fetch next batch
            if (!try self.fetchNextBatch()) {
                return null;
            }
            self.idx_result = 0;
        }
        // Move forward
        defer self.idx_result += 1;
        return self.batch[self.idx_result];
    }

    /// Fetches the next batch of messages
    /// Returns `true` if we fetched one or more messages, `false` otherwise
    fn fetchNextBatch(self: *ForwardCursor) FetchError!bool {
        var response_writer = std.Io.Writer.fixed(self.client.msg_result_buf);

        const fetch_options: std.http.Client.FetchOptions = .{
            .method = .GET,
            .location = .{ .url = self.query_cursor_fetch_url },
            .response_writer = &response_writer,
        };

        const result = self.client.http_client.fetch(fetch_options) catch |e| {
            self.logger.err()
                .msg("Failed to fetch message batch")
                .err(e)
                .intx("cursor_id", self.cursor_id)
                .log();

            return switch (e) {
                error.WriteFailed => NetworkError.SocketFailure,
                error.UnsupportedCompressionMethod, error.StreamTooLong => UsageError.UnsupportedOperation,
                else => ClientError.ClientFailure,
            };
        };

        return switch (result.status) {
            .ok => happy: {
                const batch_buffer = response_writer.buffered();

                if (batch_buffer.len < dymes_msg.constants.message_frame_header_size) {
                    // self.logger.fine()
                    //     .msg("No further results available")
                    //     .intx("cursor_id", self.cursor_id)
                    //     .log();
                    break :happy false;
                }

                var msg_start_off: usize = 0;
                var idx_msg: usize = 0;
                while (msg_start_off < batch_buffer.len) : (idx_msg += 1) {
                    if (marshal_ulids) {
                        const id_offset = @offsetOf(dymes_msg.FrameHeader, "id");
                        const correlation_id_offset = @offsetOf(dymes_msg.FrameHeader, "correlation_id");

                        const marshalled_id = ulid.fromBytes(batch_buffer[msg_start_off + id_offset ..][0..@sizeOf(Ulid)]) catch return ClientError.ClientFailure;
                        const marshalled_correlation_id = ulid.fromBytes(batch_buffer[msg_start_off + correlation_id_offset ..][0..@sizeOf(Ulid)]) catch return ClientError.ClientFailure;
                        const message_id = dymes_msg.marshalling.unmarshalUlid(marshalled_id);
                        const correlation_id = dymes_msg.marshalling.unmarshalUlid(marshalled_correlation_id);

                        @memcpy(batch_buffer[msg_start_off + id_offset .. msg_start_off + id_offset + @sizeOf(Ulid)], &message_id.bytes());
                        @memcpy(batch_buffer[msg_start_off + correlation_id_offset .. msg_start_off + correlation_id_offset + @sizeOf(Ulid)], &correlation_id.bytes());
                    }

                    const result_msg = Message.overlay(batch_buffer[msg_start_off..]);
                    result_msg.verifyChecksums() catch |e| {
                        self.logger.err()
                            .msg("Message was mangled en-route")
                            .err(e)
                            .int("msg_start_off", msg_start_off)
                            .intx("cursor_id", self.cursor_id)
                            .log();
                        return UsageError.IllegalArgument;
                    };
                    self.batch[idx_msg] = result_msg;
                    msg_start_off += result_msg.usedFrame().len;
                }

                self.idx_result = 0;
                self.num_results = idx_msg;
                self.final_batch = idx_msg < self.batch.len;
                break :happy idx_msg > 0;
            },
            .bad_request => unhappy: {
                self.logger.err()
                    .msg("Server rejected our request")
                    .int("status", result.status)
                    .intx("cursor_id", self.cursor_id)
                    .log();
                break :unhappy ClientError.ClientFailure;
            },
            else => {
                self.logger.err()
                    .msg("Unexpected status code")
                    .int("status", result.status)
                    .intx("cursor_id", self.cursor_id)
                    .log();
                return ClientError.ClientFailure;
            },
        };
    }
};

test "Client" {
    std.testing.refAllDeclsRecursive(@This());
}
