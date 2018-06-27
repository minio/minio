// +build windows

package host

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/StackExchange/wmi"
	"github.com/shirou/gopsutil/internal/common"
	process "github.com/shirou/gopsutil/process"
	"golang.org/x/sys/windows"
)

var (
	procGetSystemTimeAsFileTime = common.Modkernel32.NewProc("GetSystemTimeAsFileTime")
	osInfo                      *Win32_OperatingSystem
)

type Win32_OperatingSystem struct {
	Version        string
	Caption        string
	ProductType    uint32
	BuildNumber    string
	LastBootUpTime time.Time
}

func Info() (*InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) (*InfoStat, error) {
	ret := &InfoStat{
		OS: runtime.GOOS,
	}

	{
		hostname, err := os.Hostname()
		if err == nil {
			ret.Hostname = hostname
		}
	}

	{
		platform, family, version, err := PlatformInformationWithContext(ctx)
		if err == nil {
			ret.Platform = platform
			ret.PlatformFamily = family
			ret.PlatformVersion = version
		} else {
			return ret, err
		}
	}

	{
		boot, err := BootTime()
		if err == nil {
			ret.BootTime = boot
			ret.Uptime, _ = Uptime()
		}
	}

	{
		hostID, err := getMachineGuid()
		if err == nil {
			ret.HostID = strings.ToLower(hostID)
		}
	}

	{
		procs, err := process.Pids()
		if err == nil {
			ret.Procs = uint64(len(procs))
		}
	}

	return ret, nil
}

func getMachineGuid() (string, error) {
	var h windows.Handle
	err := windows.RegOpenKeyEx(windows.HKEY_LOCAL_MACHINE, windows.StringToUTF16Ptr(`SOFTWARE\Microsoft\Cryptography`), 0, windows.KEY_READ|windows.KEY_WOW64_64KEY, &h)
	if err != nil {
		return "", err
	}
	defer windows.RegCloseKey(h)

	const windowsRegBufLen = 74 // len(`{`) + len(`abcdefgh-1234-456789012-123345456671` * 2) + len(`}`) // 2 == bytes/UTF16
	const uuidLen = 36

	var regBuf [windowsRegBufLen]uint16
	bufLen := uint32(windowsRegBufLen)
	var valType uint32
	err = windows.RegQueryValueEx(h, windows.StringToUTF16Ptr(`MachineGuid`), nil, &valType, (*byte)(unsafe.Pointer(&regBuf[0])), &bufLen)
	if err != nil {
		return "", err
	}

	hostID := windows.UTF16ToString(regBuf[:])
	hostIDLen := len(hostID)
	if hostIDLen != uuidLen {
		return "", fmt.Errorf("HostID incorrect: %q\n", hostID)
	}

	return hostID, nil
}

func GetOSInfo() (Win32_OperatingSystem, error) {
	return GetOSInfoWithContext(context.Background())
}

func GetOSInfoWithContext(ctx context.Context) (Win32_OperatingSystem, error) {
	var dst []Win32_OperatingSystem
	q := wmi.CreateQuery(&dst, "")
	err := common.WMIQueryWithContext(ctx, q, &dst)
	if err != nil {
		return Win32_OperatingSystem{}, err
	}

	osInfo = &dst[0]

	return dst[0], nil
}

func Uptime() (uint64, error) {
	return UptimeWithContext(context.Background())
}

func UptimeWithContext(ctx context.Context) (uint64, error) {
	if osInfo == nil {
		_, err := GetOSInfoWithContext(ctx)
		if err != nil {
			return 0, err
		}
	}
	now := time.Now()
	t := osInfo.LastBootUpTime.Local()
	return uint64(now.Sub(t).Seconds()), nil
}

func bootTime(up uint64) uint64 {
	return uint64(time.Now().Unix()) - up
}

// cachedBootTime must be accessed via atomic.Load/StoreUint64
var cachedBootTime uint64

func BootTime() (uint64, error) {
	return BootTimeWithContext(context.Background())
}

func BootTimeWithContext(ctx context.Context) (uint64, error) {
	t := atomic.LoadUint64(&cachedBootTime)
	if t != 0 {
		return t, nil
	}
	up, err := Uptime()
	if err != nil {
		return 0, err
	}
	t = bootTime(up)
	atomic.StoreUint64(&cachedBootTime, t)
	return t, nil
}

func PlatformInformation() (platform string, family string, version string, err error) {
	return PlatformInformationWithContext(context.Background())
}

func PlatformInformationWithContext(ctx context.Context) (platform string, family string, version string, err error) {
	if osInfo == nil {
		_, err = GetOSInfoWithContext(ctx)
		if err != nil {
			return
		}
	}

	// Platform
	platform = strings.Trim(osInfo.Caption, " ")

	// PlatformFamily
	switch osInfo.ProductType {
	case 1:
		family = "Standalone Workstation"
	case 2:
		family = "Server (Domain Controller)"
	case 3:
		family = "Server"
	}

	// Platform Version
	version = fmt.Sprintf("%s Build %s", osInfo.Version, osInfo.BuildNumber)

	return
}

func Users() ([]UserStat, error) {
	return UsersWithContext(context.Background())
}

func UsersWithContext(ctx context.Context) ([]UserStat, error) {
	var ret []UserStat

	return ret, nil
}

func SensorsTemperatures() ([]TemperatureStat, error) {
	return SensorsTemperaturesWithContext(context.Background())
}

func SensorsTemperaturesWithContext(ctx context.Context) ([]TemperatureStat, error) {
	return []TemperatureStat{}, common.ErrNotImplementedError
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
	_, _, version, err := PlatformInformation()
	return version, err
}
