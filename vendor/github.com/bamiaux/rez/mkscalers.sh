#!bin/sh
# depends on https://github.com/jimmyfrasche/txt
cat fixedscalers.go.input | txt -json fixedscalers.go.template > fixedscalers.go
go fmt fixedscalers.go
go install -v .../rez/rezgen
rezgen -gen horizontal > hscalers_amd64.s && echo hscalers_amd64.s
rezgen -gen vertical   > vscalers_amd64.s && echo vscalers_amd64.s
