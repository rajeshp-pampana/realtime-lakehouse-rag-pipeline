{{- define "rlrp.labels" -}}
app.kubernetes.io/name: rlrp
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "rlrp.selectorLabels" -}}
app.kubernetes.io/name: rlrp
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
