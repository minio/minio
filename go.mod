module github.com/minio/minio

go 1.12

require (
	cloud.google.com/go v0.37.2
	contrib.go.opencensus.io/exporter/ocagent v0.4.10 // indirect
	github.com/Azure/azure-sdk-for-go v27.0.0+incompatible
	github.com/Azure/go-autorest v11.7.0+incompatible
	github.com/DataDog/zstd v1.3.5 // indirect
	github.com/alecthomas/participle v0.2.1
	github.com/aliyun/aliyun-oss-go-sdk v0.0.0-20190307165228-86c17b95fcd5
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/cheggaaa/pb v1.0.28
	github.com/coredns/coredns v1.4.0
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.12+incompatible
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/djherbis/atime v1.0.0
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.1.2-0.20190322152051-20337d8c3947
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/fatih/color v1.7.0
	github.com/fatih/structs v1.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef // indirect
	github.com/golang/snappy v0.0.1
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gopherjs/gopherjs v0.0.0-20190328170749-bb2674552d8f // indirect
	github.com/gorilla/handlers v1.4.0
	github.com/gorilla/mux v1.7.0
	github.com/gorilla/rpc v1.2.0+incompatible
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.8.5 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.4 // indirect
	github.com/hashicorp/go-rootcerts v1.0.0 // indirect
	github.com/hashicorp/raft v1.0.1 // indirect
	github.com/hashicorp/vault v1.1.0
	github.com/howeyc/gopass v0.0.0-20170109162249-bf9dde6d0d2c // indirect
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/jcmturner/gofork v0.0.0-20190328161633-dc7c13fece03 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.6
	github.com/klauspost/compress v1.4.1 // indirect
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/klauspost/pgzip v1.2.1
	github.com/klauspost/reedsolomon v1.9.1
	github.com/lib/pq v1.0.0
	github.com/mattn/go-isatty v0.0.7
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/miekg/dns v1.1.8
	github.com/minio/blazer v0.0.0-20171126203752-2081f5bf0465
	github.com/minio/cli v1.3.0
	github.com/minio/dsync v1.0.0
	github.com/minio/hdfs/v3 v3.0.0
	github.com/minio/highwayhash v1.0.0
	github.com/minio/lsync v0.0.0-20190207022115-a4e43e3d0887
	github.com/minio/mc v0.0.0-20190401030144-a1355e50e2e8
	github.com/minio/minio-go v0.0.0-20190327203652-5325257a208f
	github.com/minio/parquet-go v0.0.0-20190318185229-9d767baf1679
	github.com/minio/sha256-simd v0.0.0-20190328051042-05b4dd3047e5
	github.com/minio/sio v0.0.0-20190118043801-035b4ef8c449
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/nats-io/go-nats-streaming v0.4.2
	github.com/nats-io/nats v1.7.2
	github.com/nats-io/nats-streaming-server v0.14.1 // indirect
	github.com/nats-io/nkeys v0.0.2 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.7
	github.com/pkg/profile v1.3.0
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90 // indirect
	github.com/rjeczalik/notify v0.9.2
	github.com/rs/cors v1.6.0
	github.com/segmentio/go-prompt v1.2.1-0.20161017233205-f0d19b6901ad
	github.com/sirupsen/logrus v1.3.0 // indirect
	github.com/skyrings/skyring-common v0.0.0-20160929130248-d1c0bb1cbd5e
	github.com/smartystreets/assertions v0.0.0-20190401211740-f487f9de1cd3 // indirect
	github.com/smartystreets/goconvey v0.0.0-20190330032615-68dc04aab96a // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/streadway/amqp v0.0.0-20190402114354-16ed540749f6
	github.com/tidwall/gjson v1.2.1
	github.com/tidwall/pretty v0.0.0-20190325153808-1166b9ac2b65 // indirect
	github.com/tidwall/sjson v1.0.4
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.2 // indirect
	go.uber.org/atomic v1.3.2
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.1 // indirect
	golang.org/x/crypto v0.0.0-20190426145343-a29dc8fdc734
	golang.org/x/net v0.0.0-20190424112056-4829fb13d2c6
	golang.org/x/sync v0.0.0-20190423024810-112230192c58 // indirect
	golang.org/x/sys v0.0.0-20190429190828-d89cdac9e872
	golang.org/x/text v0.3.2 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	google.golang.org/api v0.3.0
	gopkg.in/Shopify/sarama.v1 v1.20.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28 // indirect
	gopkg.in/jcmturner/goidentity.v2 v2.0.0 // indirect
	gopkg.in/olivere/elastic.v5 v5.0.80
	gopkg.in/yaml.v2 v2.2.2
)
