module github.com/minio/minio

require (
	cloud.google.com/go v0.23.0
	contrib.go.opencensus.io/exporter/stackdriver v0.0.0-20180919222851-d1e19f5c23e9
	github.com/Azure/azure-sdk-for-go v0.0.0-20170922221532-21f5db4a3b31
	github.com/Azure/go-autorest v9.0.0+incompatible
	github.com/aliyun/aliyun-oss-go-sdk v0.0.0-20170925032315-6fe16293d6b7
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973
	github.com/cheggaaa/pb v0.0.0-20160713104425-73ae1d68fe0b
	github.com/client9/misspell v0.3.4 // indirect
	github.com/coredns/coredns v0.0.0-20180121192821-d4bf076ccf4e
	github.com/coreos/etcd v0.0.0-20180703215944-e4425ee79f2f
	github.com/coreos/go-systemd v0.0.0-20180525142239-a4887aeaa186
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v0.0.0-20160705203006-01aeca54ebda
	github.com/djherbis/atime v0.0.0-20170215084934-89517e96e10b
	github.com/dustin/go-humanize v0.0.0-20170228161531-259d2a102b87
	github.com/eapache/go-resiliency v0.0.0-20160104191539-b86b1ec0dd42
	github.com/eapache/go-xerial-snappy v0.0.0-20160609142408-bb955e01b934
	github.com/eapache/queue v1.1.0
	github.com/eclipse/paho.mqtt.golang v0.0.0-20170602163032-d06cc70ac43d
	github.com/elazarl/go-bindata-assetfs v0.0.0-20151224045452-57eb5e1fc594
	github.com/fatih/color v0.0.0-20170113151612-42c364ba4900
	github.com/fatih/structs v1.1.0
	github.com/fzipp/gocyclo v0.0.0-20150627053110-6acd4345c835 // indirect
	github.com/garyburd/redigo v0.0.0-20170216214944-0d253a66e6e1
	github.com/go-ini/ini v1.27.0
	github.com/go-sql-driver/mysql v0.0.0-20181218123637-c45f530f8e7f
	github.com/gogo/protobuf v0.0.0-20161220170212-84af2615df1b
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/googleapis/gax-go v2.0.0+incompatible
	github.com/gordonklaus/ineffassign v0.0.0-20180909121442-1003c8bd00dc // indirect
	github.com/gorilla/context v0.0.0-20160525203319-aed02d124ae4
	github.com/gorilla/handlers v0.0.0-20160410185317-66e6c6f01d8d
	github.com/gorilla/mux v0.0.0-20160605233521-9fa818a44c2b
	github.com/gorilla/rpc v0.0.0-20160517062331-bd3317b8f670
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20180522105215-e9c5d9645c43
	github.com/hashicorp/errwrap v0.0.0-20180715044906-d6c0cd880357
	github.com/hashicorp/go-cleanhttp v0.0.0-20171218145408-d5fe4b57a186
	github.com/hashicorp/go-multierror v0.0.0-20180717150148-3d5d8f294aa0
	github.com/hashicorp/go-retryablehttp v0.0.0-20180718195005-e651d75abec6
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90
	github.com/hashicorp/go-sockaddr v0.0.0-20190103214136-e92cdb5343bb // indirect
	github.com/hashicorp/go-version v0.0.0-20160119211326-7e3c02b30806
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/vault v0.0.0-20181121181053-d4367e581fe1
	github.com/howeyc/gopass v0.0.0-20170109162249-bf9dde6d0d2c
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/joyent/triton-go v0.0.0-20180116161911-7e6a47b300b1
	github.com/klauspost/compress v1.3.0
	github.com/klauspost/cpuid v0.0.0-20160106104451-349c67577817
	github.com/klauspost/crc32 v0.0.0-20161016154125-cb6bfca970f6
	github.com/klauspost/pgzip v0.0.0-20180606150939-90b2c57fba35
	github.com/klauspost/readahead v1.2.0
	github.com/klauspost/reedsolomon v1.8.0
	github.com/lib/pq v0.0.0-20181016162627-9eb73efc1fcc
	github.com/mattn/go-colorable v0.0.5
	github.com/mattn/go-isatty v0.0.0-20161123143637-30a891c33c7c
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/miekg/dns v0.0.0-20160726032027-db96a2b759cd
	github.com/minio/blazer v0.0.0-20171126203752-2081f5bf0465
	github.com/minio/cli v0.0.0-20170227073228-b8ae5507c0ce
	github.com/minio/dsync v0.0.0-20180124070302-439a0961af70
	github.com/minio/highwayhash v0.0.0-20181220011308-93ed73d64169
	github.com/minio/lsync v0.0.0-20170809210826-2d7c40f41402
	github.com/minio/mc v0.0.0-20160229164230-db6b4f13442b
	github.com/minio/minio-go v6.0.11+incompatible
	github.com/minio/sha256-simd v0.0.0-20170828173933-43ed500fe4d4
	github.com/minio/sio v0.0.0-20180315104056-b421622190be
	github.com/mitchellh/go-homedir v0.0.0-20161203194507-b8bc1bf76747
	github.com/mitchellh/mapstructure v0.0.0-20150717051158-281073eb9eb0
	github.com/nats-io/go-nats v0.0.0-20161120202126-6b6bf392d34d
	github.com/nats-io/go-nats-streaming v0.0.0-20161216191029-077898146bfb
	github.com/nats-io/nats v0.0.0-20160916181735-70b70be17b77
	github.com/nats-io/nuid v1.0.0
	github.com/nsqio/go-nsq v0.0.0-20181028195256-0527e80f3ba5
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pkg/errors v0.0.0-20171216070316-e881fd58d78e
	github.com/pkg/profile v0.0.0-20180809112205-057bc52a47ec
	github.com/prometheus/client_golang v0.9.2
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/prometheus/common v0.0.0-20181126121408-4724e9255275
	github.com/prometheus/procfs v0.0.0-20181204211112-1dc9a6cbc91a
	github.com/remyoudompheng/go-misc v0.0.0-20171223093740-dfc465eaf84f // indirect
	github.com/rjeczalik/notify v0.0.0-20180808203925-4e54e7fd043e
	github.com/rs/cors v0.0.0-20160617231935-a62a804a8a00
	github.com/rs/xhandler v0.0.0-20160618193221-ed27b6fd6521
	github.com/ryanuber/go-glob v0.0.0-20170128012129-256dc444b735 // indirect
	github.com/satori/uuid v0.0.0-20170321230731-5bf94b69c6b6
	github.com/segmentio/go-prompt v0.0.0-20161017233205-f0d19b6901ad
	github.com/skyrings/skyring-common v0.0.0-20160324141443-762fd2bfc12e
	github.com/streadway/amqp v0.0.0-20160311215503-2e25825abdbd
	github.com/tidwall/gjson v0.0.0-20181028154604-081192fa2e47
	github.com/tidwall/match v0.0.0-20160830173930-173748da739a
	github.com/tidwall/sjson v1.0.0
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.opencensus.io v0.15.0
	go.uber.org/atomic v1.1.0
	go.uber.org/multierr v0.0.0-20180122172545-ddea229ff1df
	go.uber.org/zap v0.0.0-20180627234335-7e7e266a8dbc
	golang.org/x/crypto v0.0.0-20180430181235-ae8bce003081
	golang.org/x/lint v0.0.0-20181217174547-8f45f776aaf1 // indirect
	golang.org/x/net v0.0.0-20181201002055-351d144fa1fc
	golang.org/x/oauth2 v0.0.0-20180821212333-d2e6202438be
	golang.org/x/sys v0.0.0-20180909124046-d0be0721c37e
	golang.org/x/text v0.3.0
	golang.org/x/time v0.0.0-20170927054726-6dc17368e09b
	golang.org/x/tools v0.0.0-20190104182027-498d95493402 // indirect
	google.golang.org/api v0.0.0-20180916000451-19ff8768a5c0
	google.golang.org/appengine v1.0.0
	google.golang.org/genproto v0.0.0-20180918203901-c3f76f3b92d1
	google.golang.org/grpc v1.14.0
	gopkg.in/Shopify/sarama.v1 v1.10.1
	gopkg.in/olivere/elastic.v5 v5.0.31
	gopkg.in/yaml.v2 v2.0.0-20160928153709-a5b47d31c556
)
