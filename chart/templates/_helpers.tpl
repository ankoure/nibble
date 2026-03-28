{{/*
Common labels applied to all resources.
*/}}
{{- define "transit-agencies.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: transit-agencies
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
