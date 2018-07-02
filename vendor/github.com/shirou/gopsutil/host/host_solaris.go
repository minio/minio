package host

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/internal/common"
)

func Info() (*InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) (*InfoStat, error) {
	result := &InfoStat{
		OS: runtime.GOOS,
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	result.Hostname = hostname

	// Parse versions from output of `uname(1)`
	uname, err := exec.LookPath("/usr/bin/uname")
	if err != nil {
		return nil, err
	}

	out, err := invoke.CommandWithContext(ctx, uname, "-srv")
	if err != nil {
		return nil, err
	}

	fields := strings.Fields(string(out))
	if len(fields) >= 1 {
		result.PlatformFamily = fields[0]
	}
	if len(fields) >= 2 {
		result.KernelVersion = fields[1]
	}
	if len(fields) == 3 {
		result.PlatformVersion = fields[2]
	}

	// Find distribution name from /etc/release
	fh, err := os.Open("/etc/release")
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	sc := bufio.NewScanner(fh)
	if sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(line, "SmartOS"):
			result.Platform = "SmartOS"
		case strings.HasPrefix(line, "OpenIndiana"):
			result.Platform = "OpenIndiana"
		case strings.HasPrefix(line, "OmniOS"):
			result.Platform = "OmniOS"
		case strings.HasPrefix(line, "Open Storage"):
			result.Platform = "NexentaStor"
		case strings.HasPrefix(line, "Solaris"):
			result.Platform = "Solaris"
		case strings.HasPrefix(line, "Oracle Solaris"):
			result.Platform = "Solaris"
		default:
			result.Platform = strings.Fields(line)[0]
		}
	}

	switch result.Platform {
	case "SmartOS":
		// If everything works, use the current zone ID as the HostID if present.
		zonename, err := exec.LookPath("/usr/bin/zonename")
		if err == nil {
			out, err := invoke.CommandWithContext(ctx, zonename)
			if err == nil {
				sc := bufio.NewScanner(bytes.NewReader(out))
				for sc.Scan() {
					line := sc.Text()

					// If we're in the global zone, rely on the hostname.
					if line == "global" {
						hostname, err := os.Hostname()
						if err == nil {
							result.HostID = hostname
						}
					} else {
						result.HostID = strings.TrimSpace(line)
						break
					}
				}
			}
		}
	}

	// If HostID is still empty, use hostid(1), which can lie to callers but at
	// this point there are no hardware facilities available.  This behavior
	// matches that of other supported OSes.
	if result.HostID == "" {
		hostID, err := exec.LookPath("/usr/bin/hostid")
		if err == nil {
			out, err := invoke.CommandWithContext(ctx, hostID)
			if err == nil {
				sc := bufio.NewScanner(bytes.NewReader(out))
				for sc.Scan() {
					line := sc.Text()
					result.HostID = strings.TrimSpace(line)
					break
				}
			}
		}
	}

	// Find the boot time and calculate uptime relative to it
	bootTime, err := BootTime()
	if err != nil {
		return nil, err
	}
	result.BootTime = bootTime
	result.Uptime = uptimeSince(bootTime)

	// Count number of processes based on the number of entries in /proc
	dirs, err := ioutil.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	result.Procs = uint64(len(dirs))

	return result, nil
}

var kstatMatch = regexp.MustCompile(`([^\s]+)[\s]+([^\s]*)`)

func BootTime() (uint64, error) {
	return BootTimeWithContext(context.Background())
}

func BootTimeWithContext(ctx context.Context) (uint64, error) {
	kstat, err := exec.LookPath("/usr/bin/kstat")
	if err != nil {
		return 0, err
	}

	out, err := invoke.CommandWithContext(ctx, kstat, "-p", "unix:0:system_misc:boot_time")
	if err != nil {
		return 0, err
	}

	kstats := kstatMatch.FindAllStringSubmatch(string(out), -1)
	if len(kstats) != 1 {
		return 0, fmt.Errorf("expected 1 kstat, found %d", len(kstats))
	}

	return strconv.ParseUint(kstats[0][2], 10, 64)
}

func Uptime() (uint64, error) {
	return UptimeWithContext(context.Background())
}

func UptimeWithContext(ctx context.Context) (uint64, error) {
	bootTime, err := BootTime()
	if err != nil {
		return 0, err
	}
	return uptimeSince(bootTime), nil
}

func uptimeSince(since uint64) uint64 {
	return uint64(time.Now().Unix()) - since
}

func Users() ([]UserStat, error) {
	return UsersWithContext(context.Background())
}

func UsersWithContext(ctx context.Context) ([]UserStat, error) {
	return []UserStat{}, common.ErrNotImplementedError
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
	// Parse versions from output of `uname(1)`
	uname, err := exec.LookPath("/usr/bin/uname")
	if err != nil {
		return "", err
	}

	out, err := invoke.CommandWithContext(ctx, uname, "-srv")
	if err != nil {
		return "", err
	}

	fields := strings.Fields(string(out))
	if len(fields) >= 2 {
		return fields[1], nil
	}
	return "", fmt.Errorf("could not get kernel version")
}

func PlatformInformation() (platform string, family string, version string, err error) {
	return PlatformInformationWithContext(context.Background())
}

func PlatformInformationWithContext(ctx context.Context) (platform string, family string, version string, err error) {
	/* This is not finished yet at all. Please contribute! */

	version, err = KernelVersion()
	if err != nil {
		return "", "", "", err
	}

	return "solaris", "solaris", version, nil
}
