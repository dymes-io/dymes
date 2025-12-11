//! Dymes HTTP Server Per-Request Context.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;
const ParseIntError = std.fmt.ParseIntError;

const httpz = @import("httpz");

const common = @import("dymes_common");

const logging = common.logging;
const rfc3339 = common.rfc3339;
const Logger = logging.Logger;

const ulid = common.ulid;
const Ulid = ulid.Ulid;
const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AllocationError = errors.AllocationError;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const CreationRequest = dymes_msg.CreationRequest;
const ImportRequest = dymes_msg.ImportRequest;

const dymes_client = @import("dymes_client");
const ClientQueryDto = dymes_client.ClientQueryDto;

const dymes_engine = @import("dymes_engine");
const AppendRequest = dymes_engine.AppendRequest;
const QueryRequest = dymes_engine.QueryRequest;
const Query = dymes_engine.Query;

const marshal_ulids: bool = false;

const component_name = "http.RequestContext";

const SharedContext = @import("SharedContext.zig");

const Self = @This();

logger: *Logger,
shared_ctx: *SharedContext,
qrb: QueryRequest.Builder,

pub fn init(arena: Allocator, shared_ctx: *SharedContext) AllocationError!Self {
    return .{
        .logger = logging.logger(component_name),
        .shared_ctx = shared_ctx,
        .qrb = try QueryRequest.Builder.init(arena),
    };
}

pub fn deinit(self: *Self) void {
    defer self.qrb.deinit();
}

pub fn handleLivenessProbe(self: *Self, _: *httpz.Request, response: *httpz.Response) !void {
    self.shared_ctx.metrics.http_liveness_probes.increment();

    response.header("Cache-Control", "no-cache,no-store,must-revalidate,no-transform");
    response.body = "Alive.";
    try response.write();
}

pub fn handleReadinessProbe(self: *Self, _: *httpz.Request, response: *httpz.Response) !void {
    self.shared_ctx.metrics.http_readiness_probes.increment();
    response.header("Cache-Control", "no-cache,no-store,must-revalidate,no-transform");
    const healthy = self.shared_ctx.health.probe();
    response.body = if (healthy) "Ready." else "Not ready.";
    response.status = if (healthy) @intFromEnum(std.http.Status.ok) else @intFromEnum(std.http.Status.service_unavailable);
}

pub fn handleCreateMsg(self: *Self, request: *httpz.Request, response: *httpz.Response) !void {
    const json_body = request.json(CreationRequest) catch |e| {
        self.respondWithError("Parsing creation request", e, response);
        return;
    };

    if (json_body) |_creation_req| {
        const msg_body = common.base64.decode(request.arena, _creation_req.encoded_msg_body) catch return error.InvalidArgument;
        defer request.arena.free(msg_body);

        var transient_kv_headers = std.StringArrayHashMap([]const u8).init(request.arena);
        defer transient_kv_headers.deinit();

        for (_creation_req.kv_entries) |_kv_pair| {
            transient_kv_headers.put(_kv_pair.key, _kv_pair.value) catch |e| {
                self.respondWithError("Building KV headers", e, response);
                return;
            };
        }

        const options: Message.Options = options_val: {
            if (_creation_req.encoded_correlation_id) |_encoded_corr_id| {
                const decoded_correlation_id = ulid.decode(_encoded_corr_id) catch |e| {
                    self.respondWithError("Decoding correlation id", e, response);
                    return;
                };
                break :options_val .{
                    .correlation_id = decoded_correlation_id,
                    .transient_kv_headers = transient_kv_headers,
                };
            } else break :options_val .{
                .transient_kv_headers = transient_kv_headers,
            };
        };

        // TODO: Client and request identifiers
        const client_id: u64 = 0x0;
        const request_no: u64 = 0x0;

        const appended_ulid = self.shared_ctx.ingester.append(
            client_id,
            request_no,
            _creation_req.channel,
            _creation_req.routing,
            msg_body,
            options,
        ) catch |e| {
            self.respondWithError("Appending message", e, response);
            return;
        };

        response.content_type = httpz.ContentType.TEXT;
        response.body = appended_ulid.encodeAlloc(response.arena) catch return AllocationError.OutOfMemory;
        response.status = @intFromEnum(std.http.Status.created);
        defer self.logger.fine()
            .msg("Message appended")
            .ulid("msg_id", appended_ulid)
            .log();
        return response.write();
    } else {
        self.respondWithError("Appending message", UsageError.MissingArgument, response);
        return;
    }
}

pub fn handleQuerySingleMsg(self: *Self, request: *httpz.Request, response: *httpz.Response) !void {
    if (request.param("msg_id")) |msg_id_raw| {
        const msg_id = ulid.decode(msg_id_raw) catch |e| {
            self.respondWithError("Extracting message identifier from request", e, response);
            return;
        };

        self.logger.fine()
            .msg("Querying message")
            .ulid("msg_id", msg_id)
            .log();

        self.qrb.withQuery(Query.single(msg_id));
        const query_request = try self.qrb.build();
        defer query_request.deinit();
        var cursor = try self.shared_ctx.engine.query(query_request);
        defer cursor.close();
        if (try cursor.next()) |message| {
            assert(@as(u128, @bitCast(message.frame_header.id)) == @as(u128, @bitCast(msg_id)));

            if (marshal_ulids) {
                const id_offset = @offsetOf(dymes_msg.FrameHeader, "id");
                const correlation_id_offset = @offsetOf(dymes_msg.FrameHeader, "correlation_id");
                const marshalled_id = dymes_msg.marshalling.marshalUlid(message.id());
                @memcpy(message.frame[id_offset .. id_offset + @sizeOf(Ulid)], &marshalled_id);
                const marshalled_correlation_id = dymes_msg.marshalling.marshalUlid(message.correlationId());
                @memcpy(message.frame[correlation_id_offset .. correlation_id_offset + @sizeOf(Ulid)], &marshalled_correlation_id);
            }

            response.content_type = httpz.ContentType.BINARY;
            response.body = message.usedFrame();
            response.status = @intFromEnum(std.http.Status.ok);
            defer self.logger.fine()
                .msg("Queried message")
                .ulid("msg_id", msg_id)
                .log();
            return response.write();
        } else {
            response.status = @intFromEnum(std.http.Status.not_found);
            self.logger.debug()
                .msg("Queried message not found")
                .ulid("msg_id", msg_id)
                .log();
        }
    } else {
        self.logger.warn()
            .msg("Single-message query without message identifier")
            .log();
        response.status = @intFromEnum(std.http.Status.bad_request);
    }
}

pub fn handleImportMsgs(self: *Self, request: *httpz.Request, response: *httpz.Response) !void {
    const json_body = request.json(ImportRequest) catch |e| {
        self.respondWithError("Parsing import request", e, response);
        return;
    };

    if (json_body) |_creation_req| {
        const msg_body = common.base64.decode(request.arena, _creation_req.encoded_msg_body) catch return error.InvalidArgument;
        defer request.arena.free(msg_body);

        var transient_kv_headers = std.StringArrayHashMap([]const u8).init(request.arena);
        defer transient_kv_headers.deinit();

        for (_creation_req.kv_entries) |_kv_pair| {
            transient_kv_headers.put(_kv_pair.key, _kv_pair.value) catch |e| {
                self.respondWithError("Building KV headers", e, response);
                return;
            };
        }

        const options: Message.Options = options_val: {
            if (_creation_req.encoded_correlation_id) |_encoded_corr_id| {
                const decoded_correlation_id = ulid.decode(_encoded_corr_id) catch |e| {
                    self.respondWithError("Decoding correlation id", e, response);
                    return;
                };
                break :options_val .{
                    .correlation_id = decoded_correlation_id,
                    .transient_kv_headers = transient_kv_headers,
                };
            } else break :options_val .{
                .transient_kv_headers = transient_kv_headers,
            };
        };

        const decoded_id = try ulid.decode(_creation_req.encoded_id);

        // TODO: Client and request identifiers
        const client_id: u64 = 0x0;
        const request_no: u64 = 0x0;

        const imported_ulid = self.shared_ctx.ingester.import(
            client_id,
            request_no,
            decoded_id,
            _creation_req.channel,
            _creation_req.routing,
            msg_body,
            options,
        ) catch |e| {
            self.respondWithError("Importing message", e, response);
            return;
        };

        response.content_type = httpz.ContentType.TEXT;
        response.body = imported_ulid.encodeAlloc(response.arena) catch return AllocationError.OutOfMemory;
        response.status = @intFromEnum(std.http.Status.created);
        defer self.logger.fine()
            .msg("Message imported")
            .ulid("msg_id", imported_ulid)
            .log();
        return response.write();
    } else {
        self.respondWithError("Imported message", UsageError.MissingArgument, response);
        return;
    }
}

pub fn handleOpenCursor(self: *Self, request: *httpz.Request, response: *httpz.Response) !void {
    const query_request = self.parseQueryRequest(request) catch |e| {
        self.respondWithError("Failed to extract query from request", e, response);
        if (request.body()) |_body| {
            self.logger.debug()
                .msg("Expected JSON query")
                .str("raw_query", _body)
                .log();
        } else {
            self.logger.debug()
                .msg("Expected JSON query, but body was empty")
                .log();
        }
        return;
    };
    defer query_request.deinit();

    var cursor = self.shared_ctx.engine.query(query_request) catch |e| {
        self.respondWithError("Failed to perform query", e, response);
        return;
    };
    errdefer cursor.close();

    const cursor_id = self.shared_ctx.acquiredCursor(cursor) catch |e| {
        self.respondWithError("Failed to allocate cursor", e, response);
        return;
    };
    errdefer self.shared_ctx.releaseCursor(cursor_id);

    response.status = @intFromEnum(std.http.Status.created);
    response.body = std.fmt.allocPrint(request.arena, "{x}", .{cursor_id}) catch return AllocationError.OutOfMemory;
    self.shared_ctx.metrics.http_cursors_open.increment();
}

pub fn handleCloseCursor(self: *Self, request: *httpz.Request, response: *httpz.Response) !void {
    const cursor_id = extractCursorId(request) catch |e| {
        self.respondWithError("Determining cursor identifier", e, response);
        return;
    };
    self.shared_ctx.releaseCursor(cursor_id);
    response.status = @intFromEnum(std.http.Status.ok);
    self.shared_ctx.metrics.http_cursors_open.decrement();
}

pub fn handleTraverseCursor(self: *Self, request: *httpz.Request, response: *httpz.Response) !void {
    const cursor_id = extractCursorId(request) catch |e| {
        self.respondWithError("Determining cursor identifier", e, response);
        return;
    };

    if (self.shared_ctx.cursor(cursor_id)) |query_cursor| {
        const kv_query = request.query() catch |e| {
            self.respondWithError("Parsing query arguments", e, response);
            return;
        };

        const max_results: u64 = blk_batch: {
            if (kv_query.get("batch_size")) |txt_max_results| {
                const batch = std.fmt.parseInt(u64, txt_max_results, 0) catch |e| {
                    self.respondWithError("Failure parsing max results query argument", e, response);
                    return;
                };
                break :blk_batch batch;
            } else {
                break :blk_batch 1;
            }
        };
        response.status = @intFromEnum(std.http.Status.ok);

        var num_results: u64 = 0;
        while (num_results < max_results) {
            const next = self.shared_ctx.engine.traverseCursor(.{ .cursor = query_cursor }) catch |e| {
                self.respondWithError("Failure while traversing cursor", e, response);
                return;
            };

            if (next) |message| {
                if (marshal_ulids) {
                    const id_offset = @offsetOf(dymes_msg.FrameHeader, "id");
                    const correlation_id_offset = @offsetOf(dymes_msg.FrameHeader, "correlation_id");
                    const marshalled_id = dymes_msg.marshalling.marshalUlid(message.id());
                    @memcpy(message.frame[id_offset .. id_offset + @sizeOf(Ulid)], &marshalled_id);
                    const marshalled_correlation_id = dymes_msg.marshalling.marshalUlid(message.correlationId());
                    @memcpy(message.frame[correlation_id_offset .. correlation_id_offset + @sizeOf(Ulid)], &marshalled_correlation_id);
                }
                try response.chunk(message.usedFrame());
                num_results += 1;
            } else {
                break;
            }
        }
    } else {
        self.respondWithError("Invalid cursor identifier", UsageError.IllegalArgument, response);
        return;
    }
}

fn extractCursorId(request: *httpz.Request) UsageError!u64 {
    if (request.param("cursor_id")) |_txt_cursor_id| {
        const cursor_id: u64 = std.fmt.parseUnsigned(u64, _txt_cursor_id, 0) catch return UsageError.IllegalArgument;
        return cursor_id;
    } else {
        return UsageError.MissingArgument;
    }
}

const QueryParseError = UsageError || AllocationError;
fn parseQueryRequest(self: *Self, request: *httpz.Request) QueryParseError!QueryRequest {
    const json_body = request.json(ClientQueryDto) catch return QueryParseError.IllegalConversion;

    if (json_body) |_cqdto| {
        const query = try Query.fromDTO(request.arena, _cqdto.query_dto);
        self.qrb.withQuery(query);
        for (_cqdto.filters) |_filter| {
            try self.qrb.withFilter(_filter);
        }

        return try self.qrb.build();
    } else {
        self.logger.err()
            .msg("Missing client query body")
            .err(UsageError.MissingArgument)
            .log();
        return UsageError.MissingArgument;
    }
}

fn respondWithError(self: *Self, context: []const u8, cause: anyerror, response: *httpz.Response) void {
    self.logger.err()
        .msg("An error occurred while processing an HTTP request")
        .err(cause)
        .ctx(context)
        .log();
    response.body = @errorName(cause);
    response.status = switch (cause) {
        error.InvalidCharacter, error.Overflow, error.DecodeWrongSize, error.IllegalArgument, error.IllegalConversion, error.MissingArgument => @intFromEnum(std.http.Status.bad_request),
        else => @intFromEnum(std.http.Status.internal_server_error),
    };
}
