## MinIO configuration YAML

MinIO now supports starting the server arguments and configuration via a YAML configuration file. This YAML configuration describes everything that can be configured in a MinIO setup, such as '--address', '--console-address' and command line arguments for the MinIO server.

Historically everything to MinIO was provided via command arguments for the hostnames and the drives via an ellipses syntax such as `minio server http://host{1...4}/disk{1...4}` this requirement added an additional burden to have sequential hostnames for us to make sure that we can provide horizontal distribution, however we have come across situations where sometimes this is not feasible and there are no easier alternatives without modifying /etc/hosts on the host system as root user.  Many times in airgapped deployments this is not allowed or requires audits and approvals.

MinIO server configuration file allows users to provide topology that allows for heterogeneous hostnames, allowing MinIO to deployed in pre-existing environments without any further OS level configurations.

### Usage

```
minio server --config config.yaml
```

Lets you start MinIO server with all inputs to start MinIO server provided via this configuration file, once the configuration file is provided all other pre-existing values on disk for configuration are overridden by the new values set in this configuration file.

Following is an example YAML configuration structure.
```yaml
version: v2
address: ":9000"
rootUser: "minioadmin"
rootPassword: "minioadmin"
console-address: ":9001"
certs-dir: "/home/user/.minio/certs/"
pools: # Specify the nodes and drives with pools
  - args:
      - "https://server-example-pool1:9000/mnt/disk{1...4}/"
      - "https://server{1...2}-pool1:9000/mnt/disk{1...4}/"
      - "https://server3-pool1:9000/mnt/disk{1...4}/"
      - "https://server4-pool1:9000/mnt/disk{1...4}/"
  - args:
      - "https://server-example-pool2:9000/mnt/disk{1...4}/"
      - "https://server{1...2}-pool2:9000/mnt/disk{1...4}/"
      - "https://server3-pool2:9000/mnt/disk{1...4}/"
      - "https://server4-pool2:9000/mnt/disk{1...4}/"
  # more args

options:
  ftp: # settings for MinIO to act as an ftp server
    address: ":8021"
    passive-port-range: "30000-40000"
  sftp: # settings for MinIO to act as an sftp server
    address: ":8022"
    ssh-private-key: "/home/user/.ssh/id_rsa"
```

If you are using the config `v1` YAML you should migrate your `pools:` field values to the following format

`v1` format
```yaml
pools: # Specify the nodes and drives with pools
  -
    - "https://server-example-pool1:9000/mnt/disk{1...4}/"
    - "https://server{1...2}-pool1:9000/mnt/disk{1...4}/"
    - "https://server3-pool1:9000/mnt/disk{1...4}/"
    - "https://server4-pool1:9000/mnt/disk{1...4}/"
```

to `v2` format

```yaml
pools:
  - args:
      - "https://server-example-pool1:9000/mnt/disk{1...4}/"
      - "https://server{1...2}-pool1:9000/mnt/disk{1...4}/"
      - "https://server3-pool1:9000/mnt/disk{1...4}/"
      - "https://server4-pool1:9000/mnt/disk{1...4}/"
    set-drive-count: 4 # Advanced option, must be used under guidance from MinIO team.
```

### Things to know

- Fields such as `version` and `pools` are mandatory, however all other fields are optional.
- Each pool expects a minimum of 2 nodes per pool, and unique non-repeating hosts for each argument.
- Each pool expects each host in this pool has the same number of drives specified as any other host.
- Mixing `local-path` and `distributed-path` is not allowed, doing so would cause MinIO to refuse starting the server.
- Ellipses and bracket notation (e.g. `{1...10}`) are allowed.

> NOTE: MinIO environmental variables still take precedence over the `config.yaml` file, however `config.yaml` is preferred over MinIO internal config KV settings via `mc admin config set alias/ <sub-system>`.

### TODO

In subsequent releases we are planning to extend this to provide things like

- Reload() of MinIO server arguments without fully restarting the process.

- Expanding 1 node at a time by automating the process of creating a new pool
  and decommissioning to provide a functionality that smaller deployments
  care about.

- Fully allow bracket notation (e.g. `{a,c,f}`) to have multiple entries on one line.