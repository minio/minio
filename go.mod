module github.com/minio/minio

go 1.18

require (
	cloud.google.com/go/storage v1.26.0
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/Shopify/sarama v1.36.0
	github.com/alecthomas/participle v0.7.1
	github.com/bcicen/jstream v1.0.1
	github.com/beevik/ntp v0.3.0
	github.com/bits-and-blooms/bloom/v3 v3.3.1
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cheggaaa/pb v1.0.29
	github.com/coredns/coredns v1.9.4
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/cosnicolaou/pbzip2 v1.0.1
	github.com/dchest/siphash v1.2.3
	github.com/djherbis/atime v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.4.1
	github.com/elastic/go-elasticsearch/v7 v7.17.7
	github.com/fatih/color v1.13.0
	github.com/felixge/fgprof v0.9.3
	github.com/fraugster/parquet-go v0.12.0
	github.com/go-ldap/ldap/v3 v3.4.4
	github.com/go-openapi/loads v0.21.2
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang-jwt/jwt/v4 v4.4.2
	github.com/gomodule/redigo v1.8.9
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/inconshreveable/mousetrap v1.0.1
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.15.11
	github.com/klauspost/cpuid/v2 v2.1.2
	github.com/klauspost/pgzip v1.2.5
	github.com/klauspost/readahead v1.4.0
	github.com/klauspost/reedsolomon v1.11.0
	github.com/lib/pq v1.10.7
	github.com/lithammer/shortuuid/v4 v4.0.0
	github.com/miekg/dns v1.1.50
	github.com/minio/cli v1.24.0
	github.com/minio/console v0.21.2-0.20221102173545-2ff44df63675
	github.com/minio/csvparser v1.0.0
	github.com/minio/dperf v0.4.2
	github.com/minio/highwayhash v1.0.2
	github.com/minio/kes v0.21.1
	github.com/minio/madmin-go v1.7.4
	github.com/minio/minio-go/v7 v7.0.43-0.20221021202758-c6319beb6b27
	github.com/minio/pkg v1.5.4
	github.com/minio/selfupdate v0.5.0
	github.com/minio/sha256-simd v1.0.0
	github.com/minio/simdjson-go v0.4.2
	github.com/minio/sio v0.3.0
	github.com/minio/xxml v0.0.3
	github.com/minio/zipindex v0.3.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/nats-io/nats-server/v2 v2.7.4
	github.com/nats-io/nats.go v1.17.0
	github.com/nats-io/stan.go v0.10.3
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.1.0
	github.com/philhofer/fwd v1.1.2-0.20210722190033-5c56ac6d0bb9
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.13.0
	github.com/prometheus/client_model v0.3.0
	github.com/prometheus/common v0.37.0
	github.com/prometheus/procfs v0.8.0
	github.com/rs/cors v1.8.2
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417
	github.com/secure-io/sio-go v0.3.1
	github.com/shirou/gopsutil/v3 v3.22.9
	github.com/streadway/amqp v1.0.0
	github.com/tinylib/msgp v1.1.7-0.20220719154719-f3635b96e483
	github.com/valyala/bytebufferpool v1.0.0
	github.com/xdg/scram v1.0.5
	github.com/yargevad/filepathx v1.0.0
	github.com/zeebo/xxh3 v1.0.2
	go.etcd.io/etcd/api/v3 v3.5.5
	go.etcd.io/etcd/client/v3 v3.5.5
	go.uber.org/atomic v1.10.0
	go.uber.org/zap v1.23.0
	golang.org/x/crypto v0.1.0
	golang.org/x/oauth2 v0.1.0
	golang.org/x/sys v0.1.0
	golang.org/x/time v0.0.0-20220722155302-e5dcc9cfc0b9
	google.golang.org/api v0.98.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.104.0 // indirect
	cloud.google.com/go/compute v1.10.0 // indirect
	cloud.google.com/go/iam v0.4.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20220621081337-cb9428e4ac1e // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/aymanbagabas/go-osc52 v1.2.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.3.3 // indirect
	github.com/charmbracelet/bubbles v0.14.0 // indirect
	github.com/charmbracelet/bubbletea v0.22.1 // indirect
	github.com/charmbracelet/lipgloss v0.6.0 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.4.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/frankban/quicktest v1.14.0 // indirect
	github.com/gdamore/encoding v1.0.0 // indirect
	github.com/gdamore/tcell/v2 v2.5.3 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.4 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.21.4 // indirect
	github.com/go-openapi/errors v0.20.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/runtime v0.24.2 // indirect
	github.com/go-openapi/spec v0.20.7 // indirect
	github.com/go-openapi/strfmt v0.21.3 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-openapi/validate v0.22.0 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/pprof v0.0.0-20220829040838-70bd9ae97f40 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.5.1 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/jwx v1.2.25 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2 // indirect
	github.com/minio/colorjson v1.0.4 // indirect
	github.com/minio/filepath v1.0.0 // indirect
	github.com/minio/mc v0.0.0-20221103000258-583d449e38cd // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/muesli/ansi v0.0.0-20211031195517-c9f0611b6c70 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.13.0 // indirect
	github.com/nats-io/jwt/v2 v2.2.1-0.20220113022732-58e87895b296 // indirect
	github.com/nats-io/nats-streaming-server v0.24.1 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/navidys/tvxwidgets v0.1.1 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.16 // indirect
	github.com/pkg/xattr v0.4.9 // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/pquerna/cachecontrol v0.1.0 // indirect
	github.com/prometheus/prom2json v1.3.2 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rivo/tview v0.0.0-20220916081518-2e69b7385a37 // indirect
	github.com/rivo/uniseg v0.4.2 // indirect
	github.com/rjeczalik/notify v0.9.2 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/tidwall/gjson v1.14.3 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.5.0 // indirect
	github.com/unrolled/secure v1.13.0 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.5 // indirect
	go.mongodb.org/mongo-driver v1.10.3 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/goleak v1.1.12 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/net v0.1.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/term v0.1.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	golang.org/x/tools v0.1.12 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20221018160656-63c7b68cfc55 // indirect
	google.golang.org/grpc v1.50.1 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/h2non/filetype.v1 v1.0.5 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
