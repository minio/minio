// +build darwin
// +build !cgo

package disk

import (
	"context"

	"github.com/shirou/gopsutil/internal/common"
)

func IOCounters(names ...string) (map[string]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), names...)
}

func IOCountersWithContext(ctx context.Context, names ...string) (map[string]IOCountersStat, error) {
	return nil, common.ErrNotImplementedError
}
