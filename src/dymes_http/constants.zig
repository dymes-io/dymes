//! Dymes HTTP constants.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const metrics = @import("dymes_common").metrics;

const client_constants = @import("dymes_client").constants;

pub const default_health_listen_address: []const u8 = "0.0.0.0";
pub const default_health_listen_port: u16 = 6502;
pub const default_request_listen_address: []const u8 = "127.0.0.1";
pub const default_request_listen_port: u16 = client_constants.default_request_port;

pub const http_path_prefix: []const u8 = client_constants.http_path_prefix;
