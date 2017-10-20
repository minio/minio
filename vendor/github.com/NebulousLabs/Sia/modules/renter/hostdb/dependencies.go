package hostdb

import (
	"net"
	"time"

	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/persist"
)

// These interfaces define the HostDB's dependencies. Using the smallest
// interface possible makes it easier to mock these dependencies in testing.
type (
	dependencies interface {
		dialTimeout(modules.NetAddress, time.Duration) (net.Conn, error)
		disrupt(string) bool
		loadFile(persist.Metadata, interface{}, string) error
		saveFileSync(persist.Metadata, interface{}, string) error
		sleep(time.Duration)
	}
)

type prodDependencies struct{}

func (prodDependencies) dialTimeout(addr modules.NetAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(addr), timeout)
}

func (prodDependencies) disrupt(string) bool { return false }

func (prodDependencies) loadFile(meta persist.Metadata, data interface{}, filename string) error {
	return persist.LoadJSON(meta, data, filename)
}

func (prodDependencies) saveFileSync(meta persist.Metadata, data interface{}, filename string) error {
	return persist.SaveJSON(meta, data, filename)
}

func (prodDependencies) sleep(d time.Duration) { time.Sleep(d) }
