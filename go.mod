module github.com/minio/minio

go 1.17

require (
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Shopify/sarama v1.27.2
	github.com/VividCortex/ewma v1.1.1
	github.com/alecthomas/participle v0.2.1
	github.com/bcicen/jstream v1.0.1
	github.com/beevik/ntp v0.3.0
	github.com/bits-and-blooms/bloom/v3 v3.0.1
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cheggaaa/pb v1.0.29
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/coredns/coredns v1.4.0
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/cosnicolaou/pbzip2 v1.0.1
	github.com/dchest/siphash v1.2.1
	github.com/djherbis/atime v1.0.0
	github.com/dswarbrick/smart v0.0.0-20190505152634-909a45200d6d
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.3.0
	github.com/elastic/go-elasticsearch/v7 v7.12.0
	github.com/fatih/color v1.13.0
	github.com/go-ldap/ldap/v3 v3.2.4
	github.com/go-openapi/loads v0.20.2
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang-jwt/jwt/v4 v4.1.0
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/inconshreveable/mousetrap v1.0.0
	github.com/jcmturner/gokrb5/v8 v8.4.2
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.13.6
	github.com/klauspost/cpuid/v2 v2.0.9
	github.com/klauspost/pgzip v1.2.5
	github.com/klauspost/readahead v1.3.1
	github.com/klauspost/reedsolomon v1.9.15
	github.com/lib/pq v1.9.0
	github.com/miekg/dns v1.1.43
	github.com/minio/cli v1.22.0
	github.com/minio/console v0.12.9
	github.com/minio/csvparser v1.0.0
	github.com/minio/highwayhash v1.0.2
	github.com/minio/kes v0.14.0
	github.com/minio/madmin-go v1.1.20
	github.com/minio/minio-go/v7 v7.0.17
	github.com/minio/parquet-go v1.1.0
	github.com/minio/pkg v1.1.9
	github.com/minio/selfupdate v0.4.0
	github.com/minio/sha256-simd v1.0.0
	github.com/minio/simdjson-go v0.2.1
	github.com/minio/sio v0.3.0
	github.com/minio/zipindex v0.2.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/montanaflynn/stats v0.6.6
	github.com/nats-io/nats-server/v2 v2.3.2
	github.com/nats-io/nats.go v1.11.1-0.20210623165838-4b75fc59ae30
	github.com/nats-io/stan.go v0.8.3
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.8
	github.com/philhofer/fwd v1.1.2-0.20210722190033-5c56ac6d0bb9
	github.com/pierrec/lz4 v2.6.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/procfs v0.7.3
	github.com/rs/cors v1.7.0
	github.com/rs/dnscache v0.0.0-20210201191234-295bba877686
	github.com/secure-io/sio-go v0.3.1
	github.com/shirou/gopsutil/v3 v3.21.9
	github.com/streadway/amqp v1.0.0
	github.com/tinylib/msgp v1.1.7-0.20211026165309-e818a1881b0e
	github.com/valyala/bytebufferpool v1.0.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/yargevad/filepathx v1.0.0
	github.com/zeebo/xxh3 v1.0.0
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20211209193657-4570a0811e8b
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sys v0.0.0-20211020174200-9d6173849985
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/api v0.58.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.94.1 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20200615164410-66371956d46c // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/apache/thrift v0.15.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200907205600-7a23bdc65eef // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/briandowns/spinner v1.16.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/emicklei/go-restful v2.9.5+incompatible // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/georgysavva/scany v0.2.7 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.1 // indirect
	github.com/go-logr/logr v0.4.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.20.0 // indirect
	github.com/go-openapi/errors v0.19.9 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.5 // indirect
	github.com/go-openapi/runtime v0.19.24 // indirect
	github.com/go-openapi/spec v0.20.3 // indirect
	github.com/go-openapi/strfmt v0.20.0 // indirect
	github.com/go-openapi/swag v0.19.14 // indirect
	github.com/go-openapi/validate v0.20.2 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/goccy/go-json v0.7.9 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/googleapis/gnostic v0.5.1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.0 // indirect
	github.com/lestrrat-go/httpcc v1.0.0 // indirect
	github.com/lestrrat-go/iter v1.0.1 // indirect
	github.com/lestrrat-go/jwx v1.2.7 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/mattn/go-colorable v0.1.10 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mb0/glob v0.0.0-20160210091149-1eb79d2de6c4 // indirect
	github.com/minio/argon2 v1.0.0 // indirect
	github.com/minio/colorjson v1.0.1 // indirect
	github.com/minio/direct-csi v1.3.5-0.20210601185811-f7776f7961bf // indirect
	github.com/minio/filepath v1.0.0 // indirect
	github.com/minio/mc v0.0.0-20211207230606-23a05f5a17f2 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/operator v0.0.0-20211011212245-31460bbbc4b7 // indirect
	github.com/minio/operator/logsearchapi v0.0.0-20211011212245-31460bbbc4b7 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.0.2 // indirect
	github.com/nats-io/nats-streaming-server v0.21.2 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pkg/profile v1.6.0 // indirect
	github.com/pkg/xattr v0.4.3 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/pquerna/cachecontrol v0.0.0-20171018203845-0dec1b30a021 // indirect
	github.com/prometheus/common v0.31.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rjeczalik/notify v0.9.2 // indirect
	github.com/rs/xid v1.3.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/tidwall/gjson v1.11.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/sjson v1.2.3 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/unrolled/secure v1.0.9 // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.0 // indirect
	go.mongodb.org/mongo-driver v1.4.6 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210928142010-c7af6a1a74c9 // indirect
	google.golang.org/grpc v1.41.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/h2non/filetype.v1 v1.0.5 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.63.2 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.5.0 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	k8s.io/api v0.21.1 // indirect
	k8s.io/apimachinery v0.21.1 // indirect
	k8s.io/client-go v0.21.1 // indirect
	k8s.io/klog/v2 v2.8.0 // indirect
	k8s.io/kube-openapi v0.0.0-20210305001622-591a79e4bda7 // indirect
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920 // indirect
	maze.io/x/duration v0.0.0-20160924141736-faac084b6075 // indirect
	sigs.k8s.io/controller-runtime v0.8.0 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.1.0 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace github.com/gomodule/redigo v2.0.0+incompatible => github.com/gomodule/redigo v1.8.5
