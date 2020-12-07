// module github.com/dingding7199/minio
// module github.com/minio/minio
module minio

go 1.14

require (
	// github.com/minio/minio/cmd v0.0.0-20201204032319-e083471ec4bc
	// github.com/minio/minio/cmd/gateway v0.0.0-20201204032319-e083471ec4bc
	cloud.google.com/go v0.39.0
	git.apache.org/thrift.git v0.13.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/go-autorest/autorest/adal v0.9.1 // indirect
	github.com/Shopify/sarama v1.24.1
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/alecthomas/participle v0.2.1
	github.com/bcicen/jstream v1.0.1
	github.com/beevik/ntp v0.3.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/cheggaaa/pb v1.0.29
	github.com/colinmarc/hdfs/v2 v2.1.1
	github.com/coredns/coredns v1.4.0
	github.com/dchest/siphash v1.2.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/djherbis/atime v1.0.0
	github.com/dswarbrick/smart v0.0.0-20190505152634-909a45200d6d
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/fatih/color v1.9.0
	github.com/fatih/structs v1.1.0
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.1.1
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/rpc v1.2.0
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/hashicorp/vault/api v1.0.4
	github.com/json-iterator/go v1.1.10
	github.com/klauspost/compress v1.11.3
	github.com/klauspost/cpuid v1.3.1
	github.com/klauspost/pgzip v1.2.5
	github.com/klauspost/readahead v1.3.1
	github.com/klauspost/reedsolomon v1.9.9
	github.com/lib/pq v1.8.0
	github.com/mattn/go-colorable v0.1.4
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.11
	github.com/miekg/dns v1.1.8
	github.com/minio/cli v1.22.0
	github.com/minio/highwayhash v1.0.0
	github.com/minio/minio v0.0.0-20201205210044-9c53cc1b8378
	github.com/minio/minio-go/v7 v7.0.6
	github.com/minio/selfupdate v0.3.1
	github.com/minio/sha256-simd v0.1.1
	github.com/minio/simdjson-go v0.1.5
	github.com/minio/sio v0.2.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mmcloughlin/avo v0.0.0-20200803215136-443f81d77104 // indirect
	github.com/montanaflynn/stats v0.5.0
	github.com/nats-io/nats-server/v2 v2.1.7
	github.com/nats-io/nats-streaming-server v0.18.0 // indirect
	github.com/nats-io/nats.go v1.10.0
	github.com/nats-io/stan.go v0.7.0
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.7
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pierrec/lz4 v2.4.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.8.0
	github.com/rjeczalik/notify v0.9.2
	github.com/rs/cors v1.7.0
	github.com/secure-io/sio-go v0.3.0
	github.com/shirou/gopsutil v2.20.9+incompatible
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/tidwall/gjson v1.3.5
	github.com/tidwall/sjson v1.0.4
	github.com/tinylib/msgp v1.1.3
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/willf/bitset v1.1.11 // indirect
	github.com/willf/bloom v2.0.3+incompatible
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
	golang.org/x/crypto v0.0.0-20201124201722-c8d3bf9c5392
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sys v0.0.0-20201015000850-e3ed0017c211
	golang.org/x/tools v0.0.0-20200929223013-bf155c11ec6f // indirect
	google.golang.org/api v0.5.0
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0
	gopkg.in/ldap.v3 v3.0.3
	gopkg.in/olivere/elastic.v5 v5.0.86
	gopkg.in/yaml.v2 v2.3.0
)

// replace github.com/minio/minio/cmd => /home/james/code/go/minio/cmd

// replace github.com/minio/minio/cmd/gateway => /home/james/code/go/minio/cmd/gateway

// module github.com/minio/minio
