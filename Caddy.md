## Setting up Proxy using Caddy.

Please download [Caddy Server](https://caddyserver.com/download)

Create a caddy configuration file as below, change the ip addresses according to your local
minio and DNS configuration.

```bash
$ ./minio --address localhost:9000 server <your_export_dir>
```

```bash
your.public.com {
    proxy / localhost:9000 {
        proxy_header Host {host}
        proxy_header X-Real-IP {remote}
        proxy_header X-Forwarded-Proto {scheme}
    }
}
```

```bash
$ ./caddy
Activating privacy features... done.
your.public.com:443
your.public.com:80
```
