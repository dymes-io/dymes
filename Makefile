# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0

.POSIX:

BUILDAH_FORMAT ?= docker
DYMES_IMAGE_NAME ?= dymes
DYMES_IMAGE_TAG ?= minimal-jazzy-jazz
TILTED_IMAGE_REF ?= $(DYMES_IMAGE_NAME):$(DYMES_IMAGE_TAG)

.PHONY: all
all: install

.PHONY: clean
clean:
	@rm -rf .zig-cache zig-out
	@mkdir .zig-cache zig-out

install: test

.PHONY: test
test:
	@scripts/build-test.sh

.PHONY: release
release:
	@scripts/build-release.sh

.PHONY: tilted
tilted: oci

.PHONY: images
images: oci

.PHONY: oci
oci: release
	@scripts/build-oci.sh $(DYMES_IMAGE_NAME):latest $(TILTED_IMAGE_REF)
