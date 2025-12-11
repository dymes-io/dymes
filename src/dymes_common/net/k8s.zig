//! k8s health probe (liveness and readiness) support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

pub const default_liveness_endpoint_name: []const u8 = "/observe/health/live";
pub const default_readiness_endpoint_name: []const u8 = "/observe/health/ready";

pub const default_probe_listen_host: []const u8 = "0.0.0.0";
pub const default_probe_listen_port: u16 = 6502;
