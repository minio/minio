# Internal changes
- KV to map{} - completed
- All '.' to '_' - completed
- Direct env to config representation, all existing ENVs become legacy (throw deprecation warning) - 70%
- Encrypt config using secret_key, if ENV is set encrypt - Separate PR, merging functionality to single PR.
- Instance name at end of ENVs for notification targets (targets follow MINIO_NOTIFY_AMQP_URL_target1=amqp://localhost:port)

```
mc admin config get|set|env (takes multi-line full-file), along with help texts. `get` inherits the value of env
```

```
mc admin service restart
```

```
mc admin config history <version>
```

```
mc admin config restore <version>
```
