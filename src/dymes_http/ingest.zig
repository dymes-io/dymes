//! Dymes Ingester.
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

const Logger = common.logging.Logger;

const dymes_engine = @import("dymes_engine");
pub const Engine = dymes_engine.Engine;
pub const Message = dymes_engine.Message;

const dymes_vsr = @import("dymes_vsr");
const OpsHandlerError = dymes_vsr.OpsHandlerError;
const VsrAppendOp = dymes_vsr.VsrAppendOp;
const VsrAppendResponse = dymes_vsr.VsrAppendResponse;
const OpsHandlerVFT = dymes_vsr.OpsHandlerVFT;
const VsrStateMachine = dymes_vsr.VsrStateMachine;
const ClientId = dymes_vsr.ClientId;
const RequestNumber = dymes_vsr.RequestNumber;
const VsrRequest = dymes_vsr.VsrRequest;

pub const Allocators = dymes_vsr.Allocators;

pub const AppendOptions = Engine.AppendOptions;
pub const AppendError = Engine.AppendError;

pub const IngesterType = enum {
    standalone,
    clustered,
};

pub const Standalone = struct {
    const component_name = "ingest.Standalone";
    logger: *Logger,
    engine: *Engine,

    pub fn init(engine: *Engine) Standalone {
        var logger = common.logging.logger(component_name);
        defer logger.debug()
            .msg("Standalone ingester started")
            .log();
        return .{
            .logger = logger,
            .engine = engine,
        };
    }

    pub fn deinit(self: *const Standalone) void {
        defer self.logger.debug()
            .msg("Standalone ingester stopped")
            .log();
    }

    pub fn append(
        self: *Standalone,
        _: ClientId,
        _: RequestNumber,
        msg_channel: u128,
        msg_routing: u128,
        msg_body: []const u8,
        options: AppendOptions,
    ) AppendError!Ulid {
        return try self.engine.append(msg_channel, msg_routing, msg_body, options);
    }

    pub fn import(
        self: *Standalone,
        _: ClientId,
        _: RequestNumber,
        msg_id: Ulid,
        msg_channel: u128,
        msg_routing: u128,
        msg_body: []const u8,
        options: AppendOptions,
    ) AppendError!Ulid {
        return try self.engine.import(msg_id, msg_channel, msg_routing, msg_body, options);
    }
};

pub const Clustered = struct {
    const component_name = "ingest.Clustered";
    logger: *Logger,
    engine: *Engine,
    allocators: Allocators,
    state_machine: *VsrStateMachine,

    pub fn init(allocators: Allocators, engine: *Engine, state_machine: *VsrStateMachine) Clustered {
        var logger = common.logging.logger(component_name);
        defer logger.debug()
            .msg("Clustered ingester started")
            .log();
        return .{
            .logger = logger,
            .engine = engine,
            .allocators = allocators,
            .state_machine = state_machine,
        };
    }

    pub fn deinit(self: *const Clustered) void {
        defer self.logger.debug()
            .msg("Clustered ingester stopped")
            .log();
    }

    pub fn append(
        self: *Clustered,
        client_id: ClientId,
        request_no: RequestNumber,
        msg_channel: u128,
        msg_routing: u128,
        msg_body: []const u8,
        options: AppendOptions,
    ) AppendError!Ulid {
        self.engine.mtx_engine.lock();
        defer self.engine.mtx_engine.unlock();

        const msg_id = try self.engine.nextMessageId();

        var msg = try Message.init(
            self.allocators.msg_frame_allocator,
            self.state_machine.state.op_no + 1, // TODO - better LSN management
            msg_id,
            msg_channel,
            msg_routing,
            msg_body,
            options,
        );
        defer msg.deinit(self.allocators.msg_frame_allocator);

        const vsr_request: VsrRequest = .{
            .client_id = client_id,
            .request_no = request_no,
            .op = .{ .append = try VsrAppendOp.init(self.allocators.msg_frame_allocator, msg) },
        };
        self.state_machine.request(vsr_request) catch |_e| {
            self.logger.err()
                .msg("Failed to append message")
                .err(_e)
                .ulid("msg_id", msg_id)
                .log();
            return AppendError.FailedToStore;
        };

        return msg_id;
    }

    pub fn import(
        self: *Clustered,
        client_id: ClientId,
        request_no: RequestNumber,
        msg_id: Ulid,
        msg_channel: u128,
        msg_routing: u128,
        msg_body: []const u8,
        options: AppendOptions,
    ) AppendError!Ulid {
        var msg = try Message.init(
            self.allocators.msg_frame_allocator,
            self.state_machine.state.op_no + 1, // TODO - better LSN management
            msg_id,
            msg_channel,
            msg_routing,
            msg_body,
            options,
        );
        defer msg.deinit(self.allocators.msg_frame_allocator);

        const vsr_request: VsrRequest = .{
            .client_id = client_id,
            .request_no = request_no,
            .op = .{ .append = try VsrAppendOp.init(self.allocators.msg_frame_allocator, msg) },
        };
        self.state_machine.request(vsr_request) catch |_e| {
            self.logger.err()
                .msg("Failed to import message")
                .err(_e)
                .ulid("msg_id", msg_id)
                .log();
            return AppendError.FailedToStore;
        };
        return msg_id;
    }
};

pub const Ingester = union(IngesterType) {
    standalone: *Standalone,
    clustered: *Clustered,

    pub fn append(
        self: *Ingester,
        client_id: ClientId,
        request_no: RequestNumber,
        msg_channel: u128,
        msg_routing: u128,
        msg_body: []const u8,
        options: AppendOptions,
    ) AppendError!Ulid {
        return switch (self.*) {
            .standalone => |_standalone_ingester| try _standalone_ingester.append(
                client_id,
                request_no,
                msg_channel,
                msg_routing,
                msg_body,
                options,
            ),
            .clustered => |_clustered_ingester| try _clustered_ingester.append(
                client_id,
                request_no,
                msg_channel,
                msg_routing,
                msg_body,
                options,
            ),
        };
    }

    pub fn import(
        self: *Ingester,
        client_id: ClientId,
        request_no: RequestNumber,
        msg_id: Ulid,
        msg_channel: u128,
        msg_routing: u128,
        msg_body: []const u8,
        options: AppendOptions,
    ) AppendError!Ulid {
        return switch (self.*) {
            .standalone => |_standalone_ingester| try _standalone_ingester.import(
                client_id,
                request_no,
                msg_id,
                msg_channel,
                msg_routing,
                msg_body,
                options,
            ),
            .clustered => |_clustered_ingester| try _clustered_ingester.import(
                client_id,
                request_no,
                msg_id,
                msg_channel,
                msg_routing,
                msg_body,
                options,
            ),
        };
    }

    pub fn deinit(self: *const Ingester) void {
        switch (self.*) {
            .standalone => |_standalone_ingester| _standalone_ingester.deinit(),
            .clustered => |_clustered_ingester| _clustered_ingester.deinit(),
        }
    }
};
