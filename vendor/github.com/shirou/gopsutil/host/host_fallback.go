// +build !darwin,!linux,!freebsd,!openbsd,!solaris,!windows

package host

import (
	"context"

	"github.com/shirou/gopsutil/internal/common"
)

func Info() (*InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) (*InfoStat, error) {
	return nil, common.ErrNotImplementedError
}

func BootTime() (uint64, error) {
	return BootTimeWithContext(context.Background())
}

func BootTimeWithContext(ctx context.Context) (uint64, error) {
	return 0, common.ErrNotImplementedError
}

func Uptime() (uint64, error) {
	return UptimeWithContext(context.Background())
}

func UptimeWithContext(ctx context.Context) (uint64, error) {
	return 0, common.ErrNotImplementedError
}

func Users() ([]UserStat, error) {
	return UsersWithContext(context.Background())
}

func UsersWithContext(ctx context.Context) ([]UserStat, error) {
	return []UserStat{}, common.ErrNotImplementedError
}

func Virtualization() (string, string, error) {
	return VirtualizationWithContext(context.Background())
}

func VirtualizationWithContext(ctx context.Context) (string, string, error) {
	return "", "", common.ErrNotImplementedError
}

func KernelVersion() (string, error) {
	return KernelVersionWithContext(context.Background())
}

func KernelVersionWithContext(ctx context.Context) (string, error) {
	return "", common.ErrNotImplementedError
}

func PlatformInformation() (string, string, string, error) {
	return PlatformInformationWithContext(context.Background())
}

func PlatformInformationWithContext(ctx context.Context) (string, string, string, error) {
	return "", "", "", common.ErrNotImplementedError
}
