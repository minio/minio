### Logging.

- `log.Fatalf`
- `log.Errorf`
- `log.Warnf`
- `log.Infof`
- `log.Debugf`

Logging is enabled across the codebase. There are three types of logging supported.

- console
- file
- syslog

```
		"console": {
			"enable": true,
			"level": "debug"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": "error"
		},
		"syslog": {
			"enable": false,
			"address": "",
			"level": "debug"
		}
```
