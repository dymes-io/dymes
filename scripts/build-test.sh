#!/usr/bin/env sh

# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0

SCRIPT_PATH=$(dirname "$0")

set -Eeu

echo Building Dymes...
zig build test -freference-trace --summary all
echo Dymes built successfully.
