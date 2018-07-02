package host

import (
	"encoding/json"

	"github.com/shirou/gopsutil/internal/common"
)

var invoke common.Invoker = common.Invoke{}

// A HostInfoStat describes the host status.
// This is not in the psutil but it useful.
type InfoStat struct {
	Hostname             string `json:"hostname"`
	Uptime               uint64 `json:"uptime"`
	BootTime             uint64 `json:"bootTime"`
	Procs                uint64 `json:"procs"`           // number of processes
	OS                   string `json:"os"`              // ex: freebsd, linux
	Platform             string `json:"platform"`        // ex: ubuntu, linuxmint
	PlatformFamily       string `json:"platformFamily"`  // ex: debian, rhel
	PlatformVersion      string `json:"platformVersion"` // version of the complete OS
	KernelVersion        string `json:"kernelVersion"`   // version of the OS kernel (if available)
	VirtualizationSystem string `json:"virtualizationSystem"`
	VirtualizationRole   string `json:"virtualizationRole"` // guest or host
	HostID               string `json:"hostid"`             // ex: uuid
}

type UserStat struct {
	User     string `json:"user"`
	Terminal string `json:"terminal"`
	Host     string `json:"host"`
	Started  int    `json:"started"`
}

type TemperatureStat struct {
	SensorKey   string  `json:"sensorKey"`
	Temperature float64 `json:"sensorTemperature"`
}

func (h InfoStat) String() string {
	s, _ := json.Marshal(h)
	return string(s)
}

func (u UserStat) String() string {
	s, _ := json.Marshal(u)
	return string(s)
}

func (t TemperatureStat) String() string {
	s, _ := json.Marshal(t)
	return string(s)
}
