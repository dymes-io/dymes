//! Dymes VSR.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

const limits = @import("dymes_vsr/limits.zig");
const wire = @import("dymes_vsr/wire.zig");
const ops = @import("dymes_vsr/ops.zig");
const vsr_msg = @import("dymes_vsr/vsr_msg.zig");
const state = @import("dymes_vsr/state.zig");
const constants = @import("dymes_vsr/constants.zig");
const transport = @import("dymes_vsr/transport.zig");
const machine = @import("dymes_vsr/machine.zig");
const proxy = @import("dymes_vsr/proxy.zig");

pub const max_append_frame_size = limits.max_append_frame_size;

pub const Allocators = wire.Allocators;

pub const ClientId = wire.ClientId;
pub const RequestNumber = wire.RequestNumber;

pub const LogSequenceNumber = wire.LogSequenceNumber;
pub const OpNumber = wire.LogSequenceNumber;
pub const CommitNumber = wire.OpNumber;
pub const ReplicaNumber = wire.ReplicaNumber;
pub const ViewNumber = wire.ViewNumber;

pub const VsrRequest = vsr_msg.VsrRequest;
pub const VsrPrepare = vsr_msg.VsrPrepare;
pub const VsrPrepareOK = vsr_msg.VsrPrepareOK;
pub const VsrReply = vsr_msg.VsrReply;
pub const VsrCommit = vsr_msg.VsrCommit;
pub const VsrStartViewChange = vsr_msg.VsrStartViewChange;
pub const VsrDoViewChange = vsr_msg.VsrDoViewChange;
pub const VsrStartView = vsr_msg.VsrStartView;
pub const VsrGetState = vsr_msg.VsrGetState;
pub const VsrNewState = vsr_msg.VsrNewState;
pub const VsrMessage = vsr_msg.VsrMessage;

pub const VsrStatus = state.VsrStatus;
pub const VsrConfiguration = state.VsrConfiguration;
pub const VsrClientTable = state.VsrClientTable;
pub const VsrState = state.VsrState;

pub const VsrAppendOp = ops.VsrAppendOp;
pub const VsrAppendResponse = ops.VsrAppendResponse;
pub const VsrOpType = ops.VsrOpType;
pub const VsrOp = ops.VsrOp;
pub const VsrOpResponse = ops.VsrOpResponse;
pub const VsrOpLog = ops.VsrOpLog;

pub const TransportCreationError = transport.TransportCreationError;
pub const TransportSendError = transport.TransportSendError;
pub const TransportReceiveError = transport.TransportReceiveError;
pub const ReplicaTransport = transport.ReplicaTransport;

pub const VsrStateMachine = machine.VsrStateMachine;

pub const VsrClientProxy = proxy.VsrClientProxy;

pub const OpsHandler = machine.OpsHandler;
pub const OpsHandlerError = machine.OpsHandlerError;
pub const OpsHandlerVFT = machine.OpsHandlerVFT;

pub const FutureUlid = proxy.FutureUlid;

test "dymes_vsr dependencies" {
    std.testing.refAllDeclsRecursive(@This());
}
