{{- $statements_length := len .statements -}}
{{- $statements_length := sub $statements_length 1 -}}
{
  "Version": "2012-10-17",
  "Statement": [
{{- range $i, $statement := .statements }}
    {
      "Effect": "Allow",
      "Action": [
"{{ $statement.actions | join "\",\n\"" }}"
      ]{{ if $statement.resources }},
      "Resource": [
"{{ $statement.resources | join "\",\n\"" }}"
      ]{{ end }}
    }{{ if lt $i $statements_length }},{{end }}
{{- end }}
  ]
}
