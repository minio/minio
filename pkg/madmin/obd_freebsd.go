package madmin

// ServerDiskHwOBDInfo - Includes usage counters, disk counters and partitions
type ServerDiskHwOBDInfo struct {
	Addr  string `json:"addr"`
	Error string `json:"error,omitempty"`
}
