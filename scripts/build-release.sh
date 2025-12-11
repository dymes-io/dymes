#!/usr/bin/env sh

# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0

set -Eeu

echo Building Dymes release...

DYMES_ARM64_PREFIX=zig-out/arm64
DYMES_AMD64_PREFIX=zig-out/amd64

#--- ARM64 build
mkdir -p ${DYMES_ARM64_PREFIX}
zig build -p ${DYMES_ARM64_PREFIX} -Dtarget=aarch64-linux-musl -Dcpu=apple_m1 -Doptimize=ReleaseSafe install

#--- AMD64 build
mkdir -p ${DYMES_AMD64_PREFIX}
zig build -p ${DYMES_AMD64_PREFIX} -Dtarget=x86_64-linux-musl -Dcpu=skylake -Doptimize=ReleaseSafe install

echo Built Dymes release.
