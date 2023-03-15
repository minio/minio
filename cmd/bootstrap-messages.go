package cmd

import (
	"fmt"
	"sync"
	"time"

	"github.com/minio/madmin-go/v2"
	"github.com/minio/minio/internal/logger"
)

type bootstrapEvent uint8

const (
	bsNoneEvent bootstrapEvent = iota
	bsBucketDNSStoreInit
	bsEtcdStoreInit
	bsEventTargetsInit
	bsLambdaTargetsInit
	bsConfigApplyDyn
	bsConfigLoad
	bsConfigLookup
	bsEtcdCheckEncrypted
	bsConfigCheckEncrypted
	bsEtcdConfigEncrypt
	bsConfigMigrateToEncrypted
	bsConfigMigrateToJSON
	bsConfigMigrateToLatest
	bsConfigMigrateToKV
	bsIAMLoadAll
	bsIAMFormatLoad
	bsIAMFormatWrite
	bsPolicyDocsLoad
	bsIAMUsersLoad
	bsIAMGroupsLoad
	bsGroupPolicyLoad
	bsServiceAcctsLoad
	bsSTSUsersLoad
	bsSTSPolicyMapLoad
	bsIAMDataLoad
	bsRegUsersLoad
	bsRegGroupsLoad
	bsUserPolicyMapLoad
	bsGroupPolicyMapLoad
	bsIAMInitStarted
	bsIAMLoadCompleted
	bsTransactionLock
	bsTransactionLockUnavailable
	bsLockUnavailable
	bsSysConfigVerify
	bsServerInit
	// must be last event type
	bsLastEvent
)

var bootstrapMsgs = [bsLastEvent]string{
	bsNoneEvent:                  "",
	bsBucketDNSStoreInit:         "initialize remote bucket DNS store",
	bsEtcdStoreInit:              "initialize etcd store",
	bsEventTargetsInit:           "initialize the event notification targets",
	bsLambdaTargetsInit:          "initialize the lambda targets",
	bsConfigApplyDyn:             "applying the dynamic configuration",
	bsConfigLoad:                 "load the configuration",
	bsConfigLookup:               "lookup the configuration",
	bsEtcdCheckEncrypted:         "check if etcd backend is encrypted",
	bsConfigCheckEncrypted:       "check if the config backend is encrypted",
	bsEtcdConfigEncrypt:          "encrypt etcd config",
	bsConfigMigrateToEncrypted:   "migrating config prefix to encrypted",
	bsConfigMigrateToJSON:        "migrate config to .minio.sys/config/config.json",
	bsConfigMigrateToLatest:      "migrate .minio.sys/config/config.json to latest version",
	bsConfigMigrateToKV:          "migrate config to KV style",
	bsIAMLoadAll:                 "loading all IAM items",
	bsPolicyDocsLoad:             "loading policy documents",
	bsRegUsersLoad:               "loading regular IAM users",
	bsRegGroupsLoad:              "loading regular IAM groups",
	bsUserPolicyMapLoad:          "loading user policy mapping",
	bsGroupPolicyMapLoad:         "loading group policy mapping",
	bsServiceAcctsLoad:           "loading service accounts",
	bsSTSUsersLoad:               "loading STS users",
	bsSTSPolicyMapLoad:           "loading STS policy mapping",
	bsIAMFormatLoad:              "Load IAM format file",
	bsIAMFormatWrite:             "Write IAM format file",
	bsIAMDataLoad:                "loading IAM data",
	bsIAMInitStarted:             "IAM initialization started",
	bsIAMLoadCompleted:           "finishing IAM loading",
	bsTransactionLock:            "trying to acquire transaction.lock",
	bsSysConfigVerify:            "verifying system configuration",
	bsServerInit:                 "initializing the server",
	bsTransactionLockUnavailable: "lock not available",
}

const bootstrapMsgsLimit = 4 << 10

type bootstrapInfo struct {
	event  bootstrapEvent
	ts     time.Time
	source string
}
type bootstrapTracer struct {
	mu   sync.RWMutex
	idx  int
	info [bootstrapMsgsLimit]bootstrapInfo
}

var globalBootstrapTracer = bootstrapTracer{}

func (bs *bootstrapTracer) Empty() bool {
	var empty bool
	bs.mu.RLock()
	empty = bs.info[0].event == bsNoneEvent
	bs.mu.RUnlock()

	return empty
}
func (bs *bootstrapTracer) Record(event bootstrapEvent) {
	if event < 0 || event > bsLastEvent {
		logger.LogOnceIf(GlobalContext, fmt.Errorf("bootstrap event %v out of range", event), fmt.Sprintf("%v", event))
		return
	}

	source := getSource(2)
	bs.mu.Lock()
	bs.info[bs.idx] = bootstrapInfo{
		event:  event,
		ts:     time.Now().UTC(),
		source: source,
	}
	bs.idx++
	if bs.idx >= bootstrapMsgsLimit {
		bs.idx = 0 // circular buffer
	}
	bs.mu.Unlock()
}

func (bs *bootstrapTracer) Events() []madmin.TraceInfo {
	var info [bootstrapMsgsLimit]bootstrapInfo
	var idx int

	bs.mu.RLock()
	idx = bs.idx
	tail := bootstrapMsgsLimit - idx
	copy(info[tail:], bs.info[:idx])
	copy(info[:tail], bs.info[idx:])
	bs.mu.RUnlock()

	traceInfo := make([]madmin.TraceInfo, 0, bootstrapMsgsLimit)
	for i := 0; i < bootstrapMsgsLimit; i++ {
		if info[i].ts.IsZero() {
			continue // skip empty events
		}
		traceInfo = append(traceInfo, madmin.TraceInfo{
			TraceType: madmin.TraceBootstrap,
			Time:      info[i].ts,
			NodeName:  globalLocalNodeName,
			FuncName:  "BOOTSTRAP",
			Message:   fmt.Sprintf("%s %s", info[i].source, bootstrapMsgs[info[i].event]),
		})
	}
	return traceInfo
}
