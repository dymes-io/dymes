#!/usr/bin/env sh

# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0

set -Eeu

if [ "$#" -ne 2 ]; then
    echo "Two arguments required"
    exit 2
fi

DYMES_IMAGE_REF="$1"
TILTED_IMAGE_REF="$2"

if [[ -e $(which docker) ]]; then
    DOCKER_EXE=docker
elif [[ -e $(which podman) ]]; then
    DOCKER_EXE=podman
else
    echo Unable to determine OCI builder
    exit 3
fi

PLATFORM="linux/$(uname -m)"
export BUILDAH_FORMAT=docker

echo Building Dymes docker image...
${DOCKER_EXE} build --platform ${PLATFORM} -t ${DYMES_IMAGE_REF} -t ${TILTED_IMAGE_REF} .
echo Built Dymes docker image.
