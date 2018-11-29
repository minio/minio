package consts

import "time"

type ReplicationState uint32

var ReplicationStaleReadTimeout = 2 * time.Second

const (
	_ ReplicationState = iota
	OldReplicationPrimary
	OldReplicationSecondary
	OldReplicationBootstrapping
	// Don't add anything here. Adding anything to this Old block would cause
	// the rest of the values to change below. This was done originally to
	// ensure no overlap between old and new values.

	ReplicationUnknown            ReplicationState = 0
	ReplicationPerformancePrimary ReplicationState = 1 << iota
	ReplicationPerformanceSecondary
	OldSplitReplicationBootstrapping
	ReplicationDRPrimary
	ReplicationDRSecondary
	ReplicationPerformanceBootstrapping
	ReplicationDRBootstrapping
	ReplicationPerformanceDisabled
	ReplicationDRDisabled
	ReplicationPerformanceStandby
)

func (r ReplicationState) string() string {
	switch r {
	case ReplicationPerformanceSecondary:
		return "secondary"
	case ReplicationPerformancePrimary:
		return "primary"
	case ReplicationPerformanceBootstrapping:
		return "bootstrapping"
	case ReplicationPerformanceDisabled:
		return "disabled"
	case ReplicationDRPrimary:
		return "primary"
	case ReplicationDRSecondary:
		return "secondary"
	case ReplicationDRBootstrapping:
		return "bootstrapping"
	case ReplicationDRDisabled:
		return "disabled"
	}

	return "unknown"
}

func (r ReplicationState) GetDRString() string {
	switch {
	case r.HasState(ReplicationDRBootstrapping):
		return ReplicationDRBootstrapping.string()
	case r.HasState(ReplicationDRPrimary):
		return ReplicationDRPrimary.string()
	case r.HasState(ReplicationDRSecondary):
		return ReplicationDRSecondary.string()
	case r.HasState(ReplicationDRDisabled):
		return ReplicationDRDisabled.string()
	default:
		return "unknown"
	}
}

func (r ReplicationState) GetPerformanceString() string {
	switch {
	case r.HasState(ReplicationPerformanceBootstrapping):
		return ReplicationPerformanceBootstrapping.string()
	case r.HasState(ReplicationPerformancePrimary):
		return ReplicationPerformancePrimary.string()
	case r.HasState(ReplicationPerformanceSecondary):
		return ReplicationPerformanceSecondary.string()
	case r.HasState(ReplicationPerformanceDisabled):
		return ReplicationPerformanceDisabled.string()
	default:
		return "unknown"
	}
}

func (r ReplicationState) HasState(flag ReplicationState) bool { return r&flag != 0 }
func (r *ReplicationState) AddState(flag ReplicationState)     { *r |= flag }
func (r *ReplicationState) ClearState(flag ReplicationState)   { *r &= ^flag }
func (r *ReplicationState) ToggleState(flag ReplicationState)  { *r ^= flag }
