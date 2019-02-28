module github.com/minio/minio

go 1.12

require (
	cloud.google.com/go v0.23.0
	contrib.go.opencensus.io/exporter/stackdriver v0.0.0-20180919222851-d1e19f5c23e9 // indirect
	github.com/Azure/azure-sdk-for-go v0.0.0-20170922221532-21f5db4a3b31
	github.com/Azure/go-autorest v9.0.0+incompatible
	github.com/alecthomas/participle v0.0.0-20190103085315-bf8340a459bd
	github.com/aliyun/aliyun-oss-go-sdk v0.0.0-20170925032315-6fe16293d6b7
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/cheggaaa/pb v0.0.0-20160713104425-73ae1d68fe0b
	github.com/coredns/coredns v0.0.0-20180121192821-d4bf076ccf4e
	github.com/coreos/etcd v0.0.0-20180703215944-e4425ee79f2f
	github.com/coreos/go-systemd v0.0.0-20180525142239-a4887aeaa186 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v0.0.0-20160705203006-01aeca54ebda
	github.com/djherbis/atime v0.0.0-20170215084934-89517e96e10b
	github.com/dustin/go-humanize v0.0.0-20170228161531-259d2a102b87
	github.com/eapache/go-resiliency v0.0.0-20160104191539-b86b1ec0dd42 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20160609142408-bb955e01b934 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/eclipse/paho.mqtt.golang v0.0.0-20181129145454-379fd9f99ba5
	github.com/elazarl/go-bindata-assetfs v0.0.0-20151224045452-57eb5e1fc594
	github.com/fatih/color v1.7.0
	github.com/fatih/structs v1.1.0
	github.com/go-ini/ini v1.27.0 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20181218123637-c45f530f8e7f
	github.com/gogo/protobuf v0.0.0-20161220170212-84af2615df1b // indirect
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/gomodule/redigo v0.0.0-20190205135352-43fe51054af5
	github.com/gorilla/context v0.0.0-20160525203319-aed02d124ae4 // indirect
	github.com/gorilla/handlers v0.0.0-20160410185317-66e6c6f01d8d
	github.com/gorilla/mux v0.0.0-20160605233521-9fa818a44c2b
	github.com/gorilla/rpc v0.0.0-20160517062331-bd3317b8f670
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20180522105215-e9c5d9645c43 // indirect
	github.com/hashicorp/go-cleanhttp v0.0.0-20171218145408-d5fe4b57a186 // indirect
	github.com/hashicorp/go-retryablehttp v0.0.0-20180718195005-e651d75abec6 // indirect
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90 // indirect
	github.com/hashicorp/go-sockaddr v1.0.1 // indirect
	github.com/hashicorp/go-version v0.0.0-20160119211326-7e3c02b30806
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/vault v0.0.0-20181121181053-d4367e581fe1
	github.com/howeyc/gopass v0.0.0-20170109162249-bf9dde6d0d2c // indirect
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/klauspost/compress v1.3.0 // indirect
	github.com/klauspost/cpuid v0.0.0-20160106104451-349c67577817 // indirect
	github.com/klauspost/crc32 v0.0.0-20161016154125-cb6bfca970f6 // indirect
	github.com/klauspost/pgzip v0.0.0-20180606150939-90b2c57fba35
	github.com/klauspost/reedsolomon v0.0.0-20190210214925-2b210cf0866d
	github.com/lib/pq v0.0.0-20181016162627-9eb73efc1fcc
	github.com/mattn/go-isatty v0.0.3
	github.com/miekg/dns v0.0.0-20160726032027-db96a2b759cd
	github.com/minio/blazer v0.0.0-20171126203752-2081f5bf0465
	github.com/minio/cli v0.0.0-20170227073228-b8ae5507c0ce
	github.com/minio/dsync v0.0.0-20190104003057-61c41ffdeea2
	github.com/minio/highwayhash v0.0.0-20181220011308-93ed73d64169
	github.com/minio/lsync v0.0.0-20170809210826-a4e43e3d0887
	github.com/minio/mc v0.0.0-20160229164230-db6b4f13442b
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/minio/parquet-go v0.0.0-20190210145630-d5e4e922da82
	github.com/minio/sha256-simd v0.0.0-20170828173933-43ed500fe4d4
	github.com/minio/sio v0.0.0-20180315104056-b421622190be
	github.com/mitchellh/go-homedir v0.0.0-20161203194507-b8bc1bf76747
	github.com/mitchellh/mapstructure v0.0.0-20150717051158-281073eb9eb0 // indirect
	github.com/nats-io/go-nats v0.0.0-20161120202126-6b6bf392d34d // indirect
	github.com/nats-io/go-nats-streaming v0.0.0-20161216191029-077898146bfb
	github.com/nats-io/nats v0.0.0-20160916181735-70b70be17b77
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/nsqio/go-nsq v0.0.0-20181028195256-0527e80f3ba5
	github.com/pkg/profile v1.2.1
	github.com/prometheus/client_golang v0.9.2
	github.com/rjeczalik/notify v0.0.0-20180808203925-4e54e7fd043e
	github.com/rs/cors v0.0.0-20190116175910-76f58f330d76
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/satori/uuid v0.0.0-20170321230731-5bf94b69c6b6 // indirect
	github.com/segmentio/go-prompt v0.0.0-20161017233205-f0d19b6901ad
	github.com/skyrings/skyring-common v0.0.0-20160324141443-762fd2bfc12e
	github.com/streadway/amqp v0.0.0-20160311215503-2e25825abdbd
	github.com/tidwall/gjson v1.1.4
	github.com/tidwall/match v0.0.0-20160830173930-173748da739a // indirect
	github.com/tidwall/sjson v1.0.0
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	go.uber.org/atomic v1.1.0
	go.uber.org/multierr v0.0.0-20180122172545-ddea229ff1df // indirect
	go.uber.org/zap v0.0.0-20180627234335-7e7e266a8dbc // indirect
	golang.org/x/crypto v0.0.0-20180430181235-ae8bce003081
	golang.org/x/net v0.0.0-20190108225652-1e06a53dbb7e
	golang.org/x/oauth2 v0.0.0-20180821212333-d2e6202438be // indirect
	golang.org/x/sys v0.0.0-20190222171317-cd391775e71e
	golang.org/x/time v0.0.0-20170927054726-6dc17368e09b
	google.golang.org/api v0.0.0-20180916000451-19ff8768a5c0
	google.golang.org/genproto v0.0.0-20180918203901-c3f76f3b92d1 // indirect
	google.golang.org/grpc v1.14.0 // indirect
	gopkg.in/Shopify/sarama.v1 v1.10.1
	gopkg.in/olivere/elastic.v5 v5.0.31
	gopkg.in/yaml.v2 v2.0.0-20160928153709-a5b47d31c556
)
