module github.com/minio/minio

go 1.17

require (
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Shopify/sarama v1.30.0
	github.com/alecthomas/participle v0.2.1
	github.com/bcicen/jstream v1.0.1
	github.com/beevik/ntp v0.3.0
	github.com/bits-and-blooms/bloom/v3 v3.0.1
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cheggaaa/pb v1.0.29
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/coredns/coredns v1.9.0
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/cosnicolaou/pbzip2 v1.0.1
	github.com/dchest/siphash v1.2.1
	github.com/djherbis/atime v1.0.0
	github.com/dswarbrick/smart v0.0.0-20190505152634-909a45200d6d
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.3.5
	github.com/elastic/go-elasticsearch/v7 v7.12.0
	github.com/fatih/color v1.13.0
	github.com/felixge/fgprof v0.9.2
	github.com/go-ldap/ldap/v3 v3.2.4
	github.com/go-openapi/loads v0.21.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang-jwt/jwt/v4 v4.3.0
	github.com/gomodule/redigo v1.8.8
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/inconshreveable/mousetrap v1.0.0
	github.com/jcmturner/gokrb5/v8 v8.4.2
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.14.4
	github.com/klauspost/cpuid/v2 v2.0.11
	github.com/klauspost/pgzip v1.2.5
	github.com/klauspost/readahead v1.4.0
	github.com/klauspost/reedsolomon v1.9.15
	github.com/lib/pq v1.10.4
	github.com/miekg/dns v1.1.46
	github.com/minio/cli v1.22.0
	github.com/minio/console v0.15.3
	github.com/minio/csvparser v1.0.0
	github.com/minio/dperf v0.3.4
	github.com/minio/highwayhash v1.0.2
	github.com/minio/kes v0.18.0
	github.com/minio/madmin-go v1.3.5
	github.com/minio/minio-go/v7 v7.0.23
	github.com/minio/parquet-go v1.1.0
	github.com/minio/pkg v1.1.20
	github.com/minio/selfupdate v0.4.0
	github.com/minio/sha256-simd v1.0.0
	github.com/minio/simdjson-go v0.4.2
	github.com/minio/sio v0.3.0
	github.com/minio/zipindex v0.2.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/montanaflynn/stats v0.6.6
	github.com/nats-io/nats-server/v2 v2.7.4
	github.com/nats-io/nats.go v1.13.1-0.20220308171302-2f2f6968e98d
	github.com/nats-io/stan.go v0.10.2
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.8
	github.com/philhofer/fwd v1.1.2-0.20210722190033-5c56ac6d0bb9
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/procfs v0.7.3
	github.com/rs/cors v1.7.0
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417
	github.com/secure-io/sio-go v0.3.1
	github.com/shirou/gopsutil/v3 v3.22.2
	github.com/streadway/amqp v1.0.0
	github.com/tinylib/msgp v1.1.7-0.20211026165309-e818a1881b0e
	github.com/valyala/bytebufferpool v1.0.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/yargevad/filepathx v1.0.0
	github.com/zeebo/xxh3 v1.0.0
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292
	golang.org/x/oauth2 v0.0.0-20220223155221-ee480838109b
	golang.org/x/sys v0.0.0-20220227234510-4e6760a101f9
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65
	google.golang.org/api v0.67.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.100.2 // indirect
	cloud.google.com/go/compute v0.1.0 // indirect
	cloud.google.com/go/iam v0.2.0 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20200615164410-66371956d46c // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/apache/thrift v0.15.0 // indirect
	github.com/armon/go-metrics v0.3.3 // indirect
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/charmbracelet/bubbles v0.10.3 // indirect
	github.com/charmbracelet/bubbletea v0.20.0 // indirect
	github.com/charmbracelet/lipgloss v0.5.0 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/frankban/quicktest v1.14.0 // indirect
	github.com/gdamore/encoding v1.0.0 // indirect
	github.com/gdamore/tcell/v2 v2.4.1-0.20210905002822-f057f0a857a1 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.21.2 // indirect
	github.com/go-openapi/errors v0.20.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.6 // indirect
	github.com/go-openapi/runtime v0.23.1 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-openapi/strfmt v0.21.2 // indirect
	github.com/go-openapi/swag v0.21.1 // indirect
	github.com/go-openapi/validate v0.21.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/goccy/go-json v0.9.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/pprof v0.0.0-20211214055906-6f57359322fd // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.0 // indirect
	github.com/lestrrat-go/httpcc v1.0.0 // indirect
	github.com/lestrrat-go/iter v1.0.1 // indirect
	github.com/lestrrat-go/jwx v1.2.19 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/minio/argon2 v1.0.0 // indirect
	github.com/minio/colorjson v1.0.1 // indirect
	github.com/minio/filepath v1.0.0 // indirect
	github.com/minio/mc v0.0.0-20220302011226-f13defa54577 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/muesli/ansi v0.0.0-20211031195517-c9f0611b6c70 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.11.1-0.20220212125758-44cd13922739 // indirect
	github.com/nats-io/jwt/v2 v2.2.1-0.20220113022732-58e87895b296 // indirect
	github.com/nats-io/nats-streaming-server v0.24.1 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/navidys/tvxwidgets v0.1.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pkg/xattr v0.4.5 // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/pquerna/cachecontrol v0.0.0-20171018203845-0dec1b30a021 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rivo/tview v0.0.0-20220216162559-96063d6082f3 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rjeczalik/notify v0.9.2 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/rs/xid v1.3.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/tidwall/gjson v1.14.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/sjson v1.2.3 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/unrolled/secure v1.10.0 // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.2 // indirect
	go.mongodb.org/mongo-driver v1.8.4 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/mod v0.5.1 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.8 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220302033224-9aa15565e42a // indirect
	google.golang.org/grpc v1.44.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/h2non/filetype.v1 v1.0.5 // indirect
	gopkg.in/ini.v1 v1.66.4 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	maze.io/x/duration v0.0.0-20160924141736-faac084b6075 // indirect
)
