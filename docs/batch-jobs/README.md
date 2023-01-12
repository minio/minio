# MinIO Batch Job
MinIO Batch jobs is an MinIO object management feature that lets you manage objects at scale. Jobs currently supported by MinIO

- Replicate objects between buckets on multiple sites

Upcoming Jobs

- Copy objects from NAS to MinIO
- Copy objects from HDFS to MinIO

## Replication Job
To perform replication via batch jobs, you create a job. The job consists of a job description YAML that describes

- Source location from where the objects must be copied from
- Target location from where the objects must be copied to
- Fine grained filtering is available to pick relevant objects from source to copy from

MinIO batch jobs framework also provides

- Retrying a failed job automatically driven by user input
- Monitoring job progress in real-time
- Send notifications upon completion or failure to user configured target

Following YAML describes the structure of a replication job, each value is documented and self-describing.

```yaml
replicate:
  apiVersion: v1
  # source of the objects to be replicated
  source:
	type: TYPE # valid values are "minio"
	bucket: BUCKET
	prefix: PREFIX
	# NOTE: if source is remote then target must be "local"
	# endpoint: ENDPOINT
	# credentials:
	#   accessKey: ACCESS-KEY
	#   secretKey: SECRET-KEY
	#   sessionToken: SESSION-TOKEN # Available when rotating credentials are used

  # target where the objects must be replicated
  target:
	type: TYPE # valid values are "minio"
	bucket: BUCKET
	prefix: PREFIX
	# NOTE: if target is remote then source must be "local"
	# endpoint: ENDPOINT
	# credentials:
	#   accessKey: ACCESS-KEY
	#   secretKey: SECRET-KEY
	#   sessionToken: SESSION-TOKEN # Available when rotating credentials are used

  # optional flags based filtering criteria
  # for all source objects
  flags:
	filter:
	  newerThan: "7d" # match objects newer than this value (e.g. 7d10h31s)
	  olderThan: "7d" # match objects older than this value (e.g. 7d10h31s)
	  createdAfter: "date" # match objects created after "date"
	  createdBefore: "date" # match objects created before "date"

	  ## NOTE: tags are not supported when "source" is remote.
	  # tags:
	  #   - key: "name"
	  #     value: "pick*" # match objects with tag 'name', with all values starting with 'pick'

	  ## NOTE: metadata filter not supported when "source" is non MinIO.
	  # metadata:
	  #   - key: "content-type"
	  #     value: "image/*" # match objects with 'content-type', with all values starting with 'image/'

	notify:
	  endpoint: "https://notify.endpoint" # notification endpoint to receive job status events
	  token: "Bearer xxxxx" # optional authentication token for the notification endpoint

	retry:
	  attempts: 10 # number of retries for the job before giving up
	  delay: "500ms" # least amount of delay between each retry
```

You can create and run multiple 'replication' jobs at a time there are no predefined limits set.

## Batch Jobs Terminology

### Job
A job is the basic unit of work for MinIO Batch Job. A job is a self describing YAML, once this YAML is submitted and evaluated - MinIO performs the requested actions on each of the objects obtained under the described criteria in job YAML file.

### Type
Type describes the job type, such as replicating objects between MinIO sites. Each job performs a single type of operation across all objects that match the job description criteria.

## Batch Jobs via Commandline
[mc](http://github.com/minio/mc) provides 'mc batch' command to create, start and manage submitted jobs.

```
NAME:
  mc batch - manage batch jobs

USAGE:
  mc batch COMMAND [COMMAND FLAGS | -h] [ARGUMENTS...]

COMMANDS:
  generate  generate a new batch job definition
  start     start a new batch job
  list, ls  list all current batch jobs
  status    summarize job events on MinIO server in real-time
  describe  describe job definition for a job
```

### Generate a job yaml
```
mc batch generate alias/ replicate
```

### Start the batch job (returns back the JID)
```
mc batch start alias/ ./replicate.yaml
Successfully start 'replicate' job `E24HH4nNMcgY5taynaPfxu` on '2022-09-26 17:19:06.296974771 -0700 PDT'
```

### List all batch jobs
```
mc batch list alias/
ID                      TYPE            USER            STARTED
E24HH4nNMcgY5taynaPfxu  replicate       minioadmin      1 minute ago
```

### List all 'replicate' batch jobs
```
mc batch list alias/ --type replicate
ID                      TYPE            USER            STARTED
E24HH4nNMcgY5taynaPfxu  replicate       minioadmin      1 minute ago
```

### Real-time 'status' for a batch job
```
mc batch status myminio/ E24HH4nNMcgY5taynaPfxu
●∙∙
Objects:        28766
Versions:       28766
Throughput:     3.0 MiB/s
Transferred:    406 MiB
Elapsed:        2m14.227222868s
CurrObjName:    share/doc/xml-core/examples/foo.xmlcatalogs
```

### 'describe' the batch job yaml.
```
mc batch describe myminio/ E24HH4nNMcgY5taynaPfxu
replicate:
  apiVersion: v1
...
```
