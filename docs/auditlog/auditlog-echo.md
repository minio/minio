# `auditlog-echo`: A tool to view MinIO Audit logs on the console

1. Run the tool with:

```
go run docs/auditlog/auditlog-echo.go
```

The listen port has a default value (8080), but can be set with the `-port` flag.

2. Configure audit logging in MinIO with for example:

```
mc admin config set myminio audit_webhook enable=on endpoint=http://localhost:8080
```

3. Make any requests to MinIO and see audit logs printed to the tool's console.
