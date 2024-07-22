# How to enable 'minio' performance profile with tuned?

## Prerequisites

Please make sure the following packages are already installed via `dnf` or `apt`: 

- `tuned`
- `curl`

### Install `tuned.conf` performance profile

#### Step 1 - download `tuned.conf` from the referenced link
```
wget https://raw.githubusercontent.com/minio/minio/master/docs/tuning/tuned.conf
```

#### Step 2 - install tuned.conf as supported performance profile on all nodes
```
sudo mkdir -p /usr/lib/tuned/minio/
sudo mv tuned.conf /usr/lib/tuned/minio
```

#### Step 3 - to enable minio performance profile on all the nodes
```
sudo tuned-adm profile minio
```
