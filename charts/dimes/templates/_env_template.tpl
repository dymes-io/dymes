# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0
{{- /*
Define env variables
*/}}
{{- define "env" }}
{{- range $key, $value := .env }}
  {{- if kindIs "map" $value}}
- name: {{ $key }}
  valueFrom:
    {{- $value | toYaml| nindent 4 }}
  {{- else }}
- name: {{ $key }}
  value: {{ $value | quote }}
  {{- end }}
{{- end }}
{{- range $name, $secret := .secrets }}
{{- if eq $secret.type "env" }}
{{- range $key := $secret.keys }}
- name: {{ $key.env_name }}
  valueFrom:
    secretKeyRef:
      name: {{ $name }}
      key: {{ $key.key_name }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
