# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0

ARG BASEIMAGELOC=alpine:20250108

FROM $BASEIMAGELOC

ARG TARGETOS
ARG TARGETARCH

RUN apk add '!usr-merge-nag'
#RUN apk upgrade --available
#RUN apk add --update curl
RUN apk add curl

RUN mkdir -p /opt/dymes/

COPY zig-out/$TARGETARCH/bin /opt/dymes/bin
COPY conf /opt/dymes/conf

# Metrics port
EXPOSE 6502

# HTTP port
EXPOSE 6510

# VSR ports
EXPOSE 1541
EXPOSE 1542
EXPOSE 1543
EXPOSE 1544
EXPOSE 1545
EXPOSE 1546
EXPOSE 1547
EXPOSE 1548
EXPOSE 1549

HEALTHCHECK --interval=1m --timeout=10s \
 CMD curl -s -X GET http://localhost:6510/observe/health/ready

WORKDIR /opt/dymes

CMD [ "--config", "/opt/dymes/conf/dymes-docker.yaml" ]
ENTRYPOINT [ "/opt/dymes/bin/dymes_node" ]
