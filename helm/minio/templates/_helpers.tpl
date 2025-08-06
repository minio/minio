{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "minio.name" -}}
  {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "minio.fullname" -}}
  {{- if .Values.fullnameOverride -}}
    {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
  {{- else -}}
    {{- $name := default .Chart.Name .Values.nameOverride -}}
    {{- if contains $name .Release.Name -}}
      {{- .Release.Name | trunc 63 | trimSuffix "-" -}}
    {{- else -}}
      {{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "minio.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for networkpolicy.
*/}}
{{- define "minio.networkPolicy.apiVersion" -}}
  {{- if semverCompare ">=1.4-0, <1.7-0" .Capabilities.KubeVersion.Version -}}
    {{- print "extensions/v1beta1" -}}
  {{- else if semverCompare ">=1.7-0, <1.16-0" .Capabilities.KubeVersion.Version -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else if semverCompare "^1.16-0" .Capabilities.KubeVersion.Version -}}
    {{- print "networking.k8s.io/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for deployment.
*/}}
{{- define "minio.deployment.apiVersion" -}}
  {{- if semverCompare "<1.9-0" .Capabilities.KubeVersion.Version -}}
    {{- print "apps/v1beta2" -}}
  {{- else -}}
    {{- print "apps/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for statefulset.
*/}}
{{- define "minio.statefulset.apiVersion" -}}
  {{- if semverCompare "<1.16-0" .Capabilities.KubeVersion.Version -}}
    {{- print "apps/v1beta2" -}}
  {{- else -}}
    {{- print "apps/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for ingress.
*/}}
{{- define "minio.ingress.apiVersion" -}}
  {{- if semverCompare "<1.14-0" .Capabilities.KubeVersion.GitVersion -}}
    {{- print "extensions/v1beta1" -}}
  {{- else if semverCompare "<1.19-0" .Capabilities.KubeVersion.GitVersion -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{- print "networking.k8s.io/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for console ingress.
*/}}
{{- define "minio.consoleIngress.apiVersion" -}}
  {{- if semverCompare "<1.14-0" .Capabilities.KubeVersion.GitVersion -}}
    {{- print "extensions/v1beta1" -}}
  {{- else if semverCompare "<1.19-0" .Capabilities.KubeVersion.GitVersion -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{- print "networking.k8s.io/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Determine secret name.
*/}}
{{- define "minio.secretName" -}}
  {{- if .Values.existingSecret -}}
    {{- .Values.existingSecret }}
  {{- else -}}
    {{- include "minio.fullname" . -}}
  {{- end -}}
{{- end -}}

{{/*
Determine name for scc role and rolebinding
*/}}
{{- define "minio.sccRoleName" -}}
  {{- printf "%s-%s" "scc" (include "minio.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Properly format optional additional arguments to MinIO binary
*/}}
{{- define "minio.extraArgs" -}}
{{- range .Values.extraArgs -}}
{{ " " }}{{ . }}
{{- end -}}
{{- end -}}

{{/*
Return the proper Docker Image Registry Secret Names
*/}}
{{- define "minio.imagePullSecrets" -}}
{{/*
Helm 2.11 supports the assignment of a value to a variable defined in a different scope,
but Helm 2.9 and 2.10 does not support it, so we need to implement this if-else logic.
Also, we can not use a single if because lazy evaluation is not an option
*/}}
{{- if .Values.global }}
{{- if .Values.global.imagePullSecrets }}
imagePullSecrets:
    {{ toYaml .Values.global.imagePullSecrets }}
{{- else if .Values.imagePullSecrets }}
imagePullSecrets:
    {{ toYaml .Values.imagePullSecrets }}
{{- end -}}
{{- else if .Values.imagePullSecrets }}
imagePullSecrets:
    {{ toYaml .Values.imagePullSecrets }}
{{- end -}}
{{- end -}}

{{/*
Formats volumeMount for MinIO TLS keys and trusted certs
*/}}
{{- define "minio.tlsKeysVolumeMount" -}}
{{- if .Values.tls.enabled }}
- name: cert-secret-volume
  mountPath: {{ .Values.certsPath }}
{{- end }}
{{- if or .Values.tls.enabled (ne .Values.trustedCertsSecret "") }}
{{- $casPath := printf "%s/CAs" .Values.certsPath | clean }}
- name: trusted-cert-secret-volume
  mountPath: {{ $casPath }}
{{- end }}
{{- end -}}

{{/*
Formats volume for MinIO TLS keys and trusted certs
*/}}
{{- define "minio.tlsKeysVolume" -}}
{{- if .Values.tls.enabled }}
- name: cert-secret-volume
  secret:
    secretName: {{ tpl .Values.tls.certSecret $ }}
    items:
    - key: {{ .Values.tls.publicCrt }}
      path: public.crt
    - key: {{ .Values.tls.privateKey }}
      path: private.key
{{- end }}
{{- if or .Values.tls.enabled (ne .Values.trustedCertsSecret "") }}
{{- $certSecret := eq .Values.trustedCertsSecret "" | ternary .Values.tls.certSecret .Values.trustedCertsSecret }}
{{- $publicCrt := eq .Values.trustedCertsSecret "" | ternary .Values.tls.publicCrt "" }}
- name: trusted-cert-secret-volume
  secret:
    secretName: {{ $certSecret }}
    {{- if ne $publicCrt "" }}
    items:
    - key: {{ $publicCrt }}
      path: public.crt
    {{- end }}
{{- end }}
{{- end -}}

{{/*
Returns the available value for certain key in an existing secret (if it exists),
otherwise it generates a random value.
*/}}
{{- define "minio.getValueFromSecret" }}
  {{- $len := (default 16 .Length) | int -}}
  {{- $obj := (lookup "v1" "Secret" .Namespace .Name).data -}}
  {{- if $obj }}
    {{- index $obj .Key | b64dec -}}
  {{- else -}}
    {{- randAlphaNum $len -}}
  {{- end -}}
{{- end }}

{{- define "minio.root.username" -}}
  {{- if .Values.rootUser }}
    {{- .Values.rootUser | toString }}
  {{- else }}
    {{- include "minio.getValueFromSecret" (dict "Namespace" .Release.Namespace "Name" (include "minio.fullname" .) "Length" 20 "Key" "rootUser") }}
  {{- end }}
{{- end -}}

{{- define "minio.root.password" -}}
  {{- if .Values.rootPassword }}
    {{- .Values.rootPassword | toString }}
  {{- else }}
    {{- include "minio.getValueFromSecret" (dict "Namespace" .Release.Namespace "Name" (include "minio.fullname" .) "Length" 40 "Key" "rootPassword") }}
  {{- end }}
{{- end -}}
