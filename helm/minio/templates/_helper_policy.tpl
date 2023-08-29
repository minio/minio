{{- $statements_length := len .statements -}}
{{- $statements_length := sub $statements_length 1 -}}
{
  "Version": "2012-10-17",
  "Statement": [
{{- range $i, $statement := .statements }}
    {
      "Effect": "{{ $statement.effect | default "Allow" }}",
      "Action": [
"{{ $statement.actions | join "\",\n\"" }}"
      ]{{ if $statement.resources }},
      "Resource": [
"{{ $statement.resources | join "\",\n\"" }}"
      ]{{ end }}
{{- if $statement.conditions }}
{{- $condition_len := len $statement.conditions }}
{{- $condition_len := sub $condition_len 1 }}
      ,
      "Condition": {
 {{- range $k,$v := $statement.conditions }}
 {{- range $operator,$object := $v }}
        "{{ $operator }}": { {{ $object }} }{{- if lt $k $condition_len }},{{- end }}
 {{- end }}{{- end }}
      }{{- end }}
    }{{ if lt $i $statements_length }},{{end }}
{{- end }}
  ]
}
