### Logging.

- `fatalIf` - wrapper function which takes error and prints jsonic error messages.
- `errorIf` - similar to fatalIf but doesn't exit on err != nil.

Supported logging targets.

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
			"level": "error"
		},
		"syslog": {
			"enable": false,
			"address": "",
			"level": "error"
		}
```
