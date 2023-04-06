module github.com/minio/minio

go 1.17

require (
	cloud.google.com/go/storage v1.21.0
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
	github.com/colinmarc/hdfs/v2 v2.3.0
	github.com/coredns/coredns v1.9.0
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/cosnicolaou/pbzip2 v1.0.1
	github.com/dchest/siphash v1.2.1
	github.com/djherbis/atime v1.0.0
	github.com/dswarbrick/smart v0.0.0-20190505152634-909a45200d6d
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.3.5
	github.com/elastic/go-elasticsearch/v7 v7.12.0
	github.com/erikdubbelboer/gspt v0.0.0-20210805194459-ce36a5128377
	github.com/fatih/color v1.13.0
	github.com/felixge/fgprof v0.9.2
	github.com/go-ldap/ldap/v3 v3.2.4
	github.com/go-openapi/loads v0.21.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang-jwt/jwt/v4 v4.2.0
	github.com/gomodule/redigo v1.8.8
	github.com/google/gops v0.3.22
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/inconshreveable/mousetrap v1.0.0
	github.com/jcmturner/gokrb5/v8 v8.4.2
	github.com/json-iterator/go v1.1.12
	github.com/juicedata/juicefs v1.0.4
	github.com/klauspost/compress v1.14.4
	github.com/klauspost/cpuid/v2 v2.0.11
	github.com/klauspost/pgzip v1.2.5
	github.com/klauspost/readahead v1.4.0
	github.com/klauspost/reedsolomon v1.9.15
	github.com/lib/pq v1.10.4
	github.com/miekg/dns v1.1.46
	github.com/minio/cli v1.22.0
	github.com/minio/console v0.15.0
	github.com/minio/csvparser v1.0.0
	github.com/minio/dperf v0.3.2
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
	github.com/nats-io/nats-server/v2 v2.7.2
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/nats-io/stan.go v0.10.2
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.8
	github.com/philhofer/fwd v1.1.2-0.20210722190033-5c56ac6d0bb9
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/procfs v0.7.3
	github.com/pyroscope-io/client v0.2.1
	github.com/rs/cors v1.7.0
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417
	github.com/secure-io/sio-go v0.3.1
	github.com/shirou/gopsutil/v3 v3.21.12
	github.com/sirupsen/logrus v1.9.0
	github.com/streadway/amqp v1.0.0
	github.com/tinylib/msgp v1.1.7-0.20211026165309-e818a1881b0e
	github.com/valyala/bytebufferpool v1.0.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/yargevad/filepathx v1.0.0
	github.com/zeebo/xxh3 v1.0.0
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.20.0
	golang.org/x/crypto v0.0.0-20220829220503-c86fa9a7ed90
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	golang.org/x/sys v0.5.0
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11
	google.golang.org/api v0.73.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.100.2 // indirect
	cloud.google.com/go/compute v1.5.0 // indirect
	cloud.google.com/go/iam v0.2.0 // indirect
	github.com/Arvintian/scs-go-sdk v1.1.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.21.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v0.8.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.3.0 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20200615164410-66371956d46c // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/IBM/ibm-cos-sdk-go v1.8.0 // indirect
	github.com/NetEase-Object-Storage/nos-golang-sdk v0.0.0-20191125093154-335c2b73bf6b // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/aliyun/aliyun-oss-go-sdk v2.2.2+incompatible // indirect
	github.com/apache/thrift v0.15.0 // indirect
	github.com/armon/go-metrics v0.3.3 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200907205600-7a23bdc65eef // indirect
	github.com/aws/aws-sdk-go v1.43.26 // indirect
	github.com/baidubce/bce-sdk-go v0.9.111 // indirect
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/ceph/go-ceph v0.4.0 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/charmbracelet/bubbles v0.10.0 // indirect
	github.com/charmbracelet/bubbletea v0.19.3 // indirect
	github.com/charmbracelet/lipgloss v0.5.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/containerd/console v1.0.2 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/deckarep/golang-set v1.8.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/dgraph-io/badger/v3 v3.2103.5 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/frankban/quicktest v1.14.0 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.1 // indirect
	github.com/go-logr/logr v1.2.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.21.2 // indirect
	github.com/go-openapi/errors v0.20.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.6 // indirect
	github.com/go-openapi/runtime v0.21.1 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-openapi/strfmt v0.21.1 // indirect
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/go-openapi/validate v0.20.3 // indirect
	github.com/go-redis/redis/v8 v8.11.5 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/goccy/go-json v0.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/pprof v0.0.0-20211214055906-6f57359322fd // indirect
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/hashicorp/consul/api v1.12.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v1.2.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/serf v0.9.6 // indirect
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.21.12+incompatible // indirect
	github.com/hungys/go-lz4 v0.0.0-20170805124057-19ff7f07f099 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/juju/ratelimit v1.0.1 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/ks3sdklib/aws-sdk-go v1.1.4 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.0 // indirect
	github.com/lestrrat-go/httpcc v1.0.0 // indirect
	github.com/lestrrat-go/iter v1.0.1 // indirect
	github.com/lestrrat-go/jwx v1.2.14 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mattn/go-sqlite3 v2.0.1+incompatible // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/minio/argon2 v1.0.0 // indirect
	github.com/minio/colorjson v1.0.1 // indirect
	github.com/minio/filepath v1.0.0 // indirect
	github.com/minio/mc v0.0.0-20220204044644-e048c85d71a7 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/operator v0.0.0-20220110040724-a5d59a342b7f // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mozillazg/go-httpheader v0.2.1 // indirect
	github.com/muesli/ansi v0.0.0-20211018074035-2e021307bc4b // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.11.1-0.20220204035834-5ac8409525e0 // indirect
	github.com/nats-io/jwt/v2 v2.2.1-0.20220113022732-58e87895b296 // indirect
	github.com/nats-io/nats-streaming-server v0.24.1 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ncw/swift v1.0.53 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pengsrc/go-shared v0.2.1-0.20190131101655-1999055a4a14 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c // indirect
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pingcap/kvproto v0.0.0-20221026112947-f8d61344b172 // indirect
	github.com/pingcap/log v1.1.1-0.20221015072633-39906604fb81 // indirect
	github.com/pkg/profile v1.6.0 // indirect
	github.com/pkg/sftp v1.13.4 // indirect
	github.com/pkg/xattr v0.4.6 // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/pquerna/cachecontrol v0.0.0-20171018203845-0dec1b30a021 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/qingstor/qingstor-sdk-go/v4 v4.4.0 // indirect
	github.com/qiniu/go-sdk/v7 v7.11.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rjeczalik/notify v0.9.2 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/rs/xid v1.3.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	github.com/studio-b12/gowebdav v0.0.0-20220128162035-c7b1ff8a5e62 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tencentyun/cos-go-sdk-v5 v0.7.34 // indirect
	github.com/tidwall/gjson v1.12.1 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/sjson v1.2.3 // indirect
	github.com/tikv/client-go/v2 v2.0.2 // indirect
	github.com/tikv/pd/client v0.0.0-20221031025758-80f0d8ca4d07 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	github.com/unrolled/secure v1.0.9 // indirect
	github.com/upyun/go-sdk/v3 v3.0.2 // indirect
	github.com/urfave/cli/v2 v2.4.0 // indirect
	github.com/vbauerster/mpb/v7 v7.0.3 // indirect
	github.com/viki-org/dnscache v0.0.0-20130720023526-c70c1f23c5d8 // indirect
	github.com/volcengine/ve-tos-golang-sdk/v2 v2.5.1 // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd v3.3.27+incompatible // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.2 // indirect
	go.mongodb.org/mongo-driver v1.7.5 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/term v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/tools v0.1.12 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220310185008-1973136f34c6 // indirect
	google.golang.org/grpc v1.45.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/h2non/filetype.v1 v1.0.5 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.66.3 // indirect
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/api v0.23.3 // indirect
	k8s.io/apimachinery v0.23.3 // indirect
	k8s.io/client-go v0.23.3 // indirect
	k8s.io/klog/v2 v2.40.1 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	k8s.io/utils v0.0.0-20211116205334-6203023598ed // indirect
	maze.io/x/duration v0.0.0-20160924141736-faac084b6075 // indirect
	sigs.k8s.io/controller-runtime v0.8.0 // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
	xorm.io/builder v0.3.7 // indirect
	xorm.io/xorm v1.0.7 // indirect
)

replace github.com/vbauerster/mpb/v7 v7.0.3 => github.com/juicedata/mpb/v7 v7.0.4-0.20220719014258-68df1356cfba

replace xorm.io/xorm v1.0.7 => gitea.com/davies/xorm v1.0.8-0.20220528043536-552d84d1b34a

replace maze.io/x/duration v0.0.0-20160924141736-faac084b6075 => github.com/juicedata/duration v0.0.0-20220916024255-e5d060e609c4

replace github.com/huaweicloud/huaweicloud-sdk-go-obs v3.21.12+incompatible => github.com/juicedata/huaweicloud-sdk-go-obs v3.22.12-0.20230228031208-386e87b5c091+incompatible
