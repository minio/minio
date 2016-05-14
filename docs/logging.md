### Logging.

- `log.Fatalf`
- `log.Errorf`
- `log.Warnf`
- `log.Infof`

Logging is enabled across the codebase. There are three types of logging supported.

- console
- file
- syslog

Sample logger section from `~/.minio/config.json`
```
		"console": {
			"enable": true,
			"level": "error"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": "trace"
		},
		"syslog": {
			"enable": false,
			"address": "",
			"level": "info"
		}
```
