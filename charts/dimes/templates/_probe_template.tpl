# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0
{{- /*
Define a probe
*/}}
{{- define "probe" }}
{{- if .httpGet }}
httpGet:
  port: {{ .httpGet.port }}
  path: {{ .httpGet.path }}
{{- if .httpGet.httpHeaders }}
  httpHeaders:
{{- range $httpHeaders := .httpGet.httpHeaders }}
  - name: {{$httpHeaders.name}}
    value: {{$httpHeaders.value}}
{{- end}}
{{- end}}
{{- else}}
tcpSocket:
  port: {{ .port }}
{{- end}}
{{- if .initialDelaySeconds }}
initialDelaySeconds: {{ .initialDelaySeconds }}
{{- end }}
{{- if .periodSeconds }}
periodSeconds: {{ .periodSeconds }}
{{- end }}
{{- if .timeoutSeconds }}
timeoutSeconds: {{ .timeoutSeconds }}
{{- end }}
{{- if .successThreshold }}
successThreshold: {{ .successThreshold }}
{{- end }}
{{- if .failureThreshold }}
failureThreshold: {{ .failureThreshold }}
{{- end }}
{{- end }}
