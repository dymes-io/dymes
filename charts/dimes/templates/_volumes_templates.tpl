# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0
{{- /*
Define volumes
*/}}
{{- define "volumes" }}
{{- range $name, $volume := .volumes }}
- name: {{ $name }}
  {{- toYaml $volume | nindent 2}}
{{- end }}
{{- end }}

{{- /*
Define volume mounts
*/}}
{{- define "volume-mounts" }}
{{- range $name, $volumeMount := .volumeMounts }}
- name: {{ $name }}
  mountPath: {{ $volumeMount.mountPath }}
  {{- if $volumeMount.subPath }}
  subPath: {{ $volumeMount.subPath }}
  {{- end }}
{{- end }}
{{- end }}
