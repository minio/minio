module github.com/minio/minio

go 1.19

require (
	cloud.google.com/go/storage v1.33.0
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/IBM/sarama v1.41.2
	github.com/alecthomas/participle v0.7.1
	github.com/bcicen/jstream v1.0.1
	github.com/beevik/ntp v1.3.0
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/cheggaaa/pb v1.0.29
	github.com/coredns/coredns v1.11.1
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/cosnicolaou/pbzip2 v1.0.2
	github.com/dchest/siphash v1.2.3
	github.com/djherbis/atime v1.1.0
	github.com/dustin/go-humanize v1.0.1
	github.com/eclipse/paho.mqtt.golang v1.4.3
	github.com/elastic/go-elasticsearch/v7 v7.17.10
	github.com/fatih/color v1.15.0
	github.com/felixge/fgprof v0.9.3
	github.com/fraugster/parquet-go v0.12.0
	github.com/go-ldap/ldap/v3 v3.4.6
	github.com/go-openapi/loads v0.21.2
	github.com/go-sql-driver/mysql v1.7.1
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/gomodule/redigo v1.8.9
	github.com/google/uuid v1.3.1
	github.com/hashicorp/golang-lru v1.0.2
	github.com/inconshreveable/mousetrap v1.1.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.17.0
	github.com/klauspost/cpuid/v2 v2.2.5
	github.com/klauspost/filepathx v1.1.1
	github.com/klauspost/pgzip v1.2.6
	github.com/klauspost/readahead v1.4.0
	github.com/klauspost/reedsolomon v1.11.8
	github.com/lib/pq v1.10.9
	github.com/lithammer/shortuuid/v4 v4.0.0
	github.com/miekg/dns v1.1.56
	github.com/minio/cli v1.24.2
	github.com/minio/console v0.39.0
	github.com/minio/csvparser v1.0.0
	github.com/minio/dnscache v0.1.1
	github.com/minio/dperf v0.5.0
	github.com/minio/highwayhash v1.0.2
	github.com/minio/kes-go v0.2.0
	github.com/minio/madmin-go/v3 v3.0.20
	github.com/minio/minio-go/v7 v7.0.63
	github.com/minio/mux v1.9.0
	github.com/minio/pkg/v2 v2.0.2
	github.com/minio/selfupdate v0.6.0
	github.com/minio/sha256-simd v1.0.1
	github.com/minio/simdjson-go v0.4.5
	github.com/minio/sio v0.3.1
	github.com/minio/xxml v0.0.3
	github.com/minio/zipindex v0.3.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/nats-io/nats-server/v2 v2.9.20
	github.com/nats-io/nats.go v1.30.0
	github.com/nats-io/stan.go v0.10.4
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.1.0
	github.com/philhofer/fwd v1.1.2
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.6
	github.com/prometheus/client_golang v1.16.0
	github.com/prometheus/client_model v0.4.0
	github.com/prometheus/common v0.44.0
	github.com/prometheus/procfs v0.11.1
	github.com/rabbitmq/amqp091-go v1.8.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rs/cors v1.10.0
	github.com/secure-io/sio-go v0.3.1
	github.com/shirou/gopsutil/v3 v3.23.8
	github.com/tidwall/gjson v1.16.0
	github.com/tinylib/msgp v1.1.8
	github.com/valyala/bytebufferpool v1.0.0
	github.com/xdg/scram v1.0.5
	github.com/zeebo/xxh3 v1.0.2
	go.etcd.io/etcd/api/v3 v3.5.9
	go.etcd.io/etcd/client/v3 v3.5.9
	go.uber.org/atomic v1.11.0
	go.uber.org/zap v1.26.0
	goftp.io/server/v2 v2.0.1
	golang.org/x/crypto v0.13.0
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9
	golang.org/x/oauth2 v0.12.0
	golang.org/x/sys v0.12.0
	golang.org/x/time v0.3.0
	google.golang.org/api v0.142.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	aead.dev/mem v0.2.0 // indirect
	aead.dev/minisign v0.2.0 // indirect
	cloud.google.com/go v0.110.8 // indirect
	cloud.google.com/go/compute v1.23.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.2 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20221128193559-754e69321358 // indirect
	github.com/apache/thrift v0.19.0 // indirect
	github.com/armon/go-metrics v0.4.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/charmbracelet/bubbles v0.16.1 // indirect
	github.com/charmbracelet/bubbletea v0.24.2 // indirect
	github.com/charmbracelet/lipgloss v0.8.0 // indirect
	github.com/containerd/console v1.0.4-0.20230313162750-1ae8d489ac81 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/eapache/go-resiliency v1.4.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/frankban/quicktest v1.14.3 // indirect
	github.com/gdamore/encoding v1.0.0 // indirect
	github.com/gdamore/tcell/v2 v2.6.0 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.5 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/analysis v0.21.4 // indirect
	github.com/go-openapi/errors v0.20.4 // indirect
	github.com/go-openapi/jsonpointer v0.20.0 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/runtime v0.26.0 // indirect
	github.com/go-openapi/spec v0.20.9 // indirect
	github.com/go-openapi/strfmt v0.21.7 // indirect
	github.com/go-openapi/swag v0.22.4 // indirect
	github.com/go-openapi/validate v0.22.1 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/pprof v0.0.0-20230912144702-c363fe2c2ed8 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.1 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-hclog v1.2.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jedib0t/go-pretty/v6 v6.4.7 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/juju/ratelimit v1.0.2 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/jwx v1.2.26 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.11 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/colorjson v1.0.6 // indirect
	github.com/minio/filepath v1.0.0 // indirect
	github.com/minio/mc v0.0.0-20230922050746-ae05d451739b // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/pkg v1.7.5 // indirect
	github.com/minio/websocket v1.6.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/muesli/ansi v0.0.0-20230316100256-276c6243b2f6 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/nats-io/jwt/v2 v2.4.1 // indirect
	github.com/nats-io/nats-streaming-server v0.24.3 // indirect
	github.com/nats-io/nkeys v0.4.5 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/navidys/tvxwidgets v0.4.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pkg/xattr v0.4.9 // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/pquerna/cachecontrol v0.2.0 // indirect
	github.com/prometheus/prom2json v1.3.3 // indirect
	github.com/rivo/tview v0.0.0-20230916092115-0ad06c2ea3dd // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/rjeczalik/notify v0.9.3 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/unrolled/secure v1.13.0 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.mongodb.org/mongo-driver v1.12.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/term v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/tools v0.13.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20230920204549-e6e6cdab5c13 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230920204549-e6e6cdab5c13 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230920204549-e6e6cdab5c13 // indirect
	google.golang.org/grpc v1.58.2 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/h2non/filetype.v1 v1.0.5 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
