// +build !darwin,!linux,!freebsd,!openbsd,!windows,!solaris

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

func Partitions(all bool) ([]PartitionStat, error) {
	return PartitionsWithContext(context.Background(), all)
}

func PartitionsWithContext(ctx context.Context, all bool) ([]PartitionStat, error) {
	return []PartitionStat{}, common.ErrNotImplementedError
}

func Usage(path string) (*UsageStat, error) {
	return UsageWithContext(context.Background(), path)
}

func UsageWithContext(ctx context.Context, path string) (*UsageStat, error) {
	return nil, common.ErrNotImplementedError
}
