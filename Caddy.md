### Proxy using Caddy.

Please download [Caddy Server](https://caddyserver.com/download)

Create a caddy configuration file as below, change the ip addresses according to your local
minio and DNS configuration.

```bash
your.public.com {
    proxy / 10.0.1.3:9000 {
        proxy_header Host {host}
        proxy_header X-Real-IP {remote}
        proxy_header X-Forwarded-Proto {scheme}
    }
    tls off
}
```

```bash
$ ./caddy
Activating privacy features... done.
your.public.com
```
