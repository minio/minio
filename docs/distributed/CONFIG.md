
## MinIO configuration

### Using a file 

It is possible to start a distributed cluster using a yaml file, e.g:
	```
		./minio server --config minio.yaml
	```

The current yaml structure is below

```
version: v1
address: ':9000'
console-address: ':9001'
certs-dir: '/home/user/.minio/certs/'
pools: # specify the nodes and drives with pools
  -
    - 'https://server1-pool1:9000/mnt/disk{1...4}/'
    - 'https://server2-pool1:9000/mnt/disk{1...4}/'
    - 'https://server3-pool1:9000/mnt/disk{1...4}/'
    - 'https://server4-pool1:9000/mnt/disk{1...4}/'
  -
    - 'https://server1-pool2:9000/mnt/disk{1...4}/'
    - 'https://server2-pool2:9000/mnt/disk{1...4}/'
    - 'https://server3-pool2:9000/mnt/disk{1...4}/'
    - 'https://server4-pool2:9000/mnt/disk{1...4}/'
options:
  ftp: # settings for MinIO to act as an ftp server
    address: ':8021'
    passive-port-range: '30000-40000'
  sftp: # settings for MinIO to act as an sftp server
    address: ':8022'
    ssh-private-key: '/home/user/.ssh/id_rsa'

```

All fields are non-mandatory except `version` and `pools`. If a field is omitted, the default value will be used.

A pool consists of a set of different disk paths. A path can be local (/mnt/disk1) or URL (e.g. https://node20/mnt/disk1/)

It is possible to you use ellipses (e.g. `{1...10}`) or bracket sequences (e.g. `{a,c,f}`) to have multiple entries in one line.

If you run MinIO in a distributed mode, all pools description need to be the same to avoid user induced errors.


### Using the environment variables

TBD

### Using 'mc admin config set'

TBD


