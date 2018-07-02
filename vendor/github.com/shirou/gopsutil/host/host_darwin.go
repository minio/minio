// +build darwin

package host

import (
	"bytes"
	"context"
	"encoding/binary"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shirou/gopsutil/internal/common"
	"github.com/shirou/gopsutil/process"
)

// from utmpx.h
const USER_PROCESS = 7

func Info() (*InfoStat, error) {
	return InfoWithContext(context.Background())
}

func InfoWithContext(ctx context.Context) (*InfoStat, error) {
	ret := &InfoStat{
		OS:             runtime.GOOS,
		PlatformFamily: "darwin",
	}

	hostname, err := os.Hostname()
	if err == nil {
		ret.Hostname = hostname
	}

	uname, err := exec.LookPath("uname")
	if err == nil {
		out, err := invoke.CommandWithContext(ctx, uname, "-r")
		if err == nil {
			ret.KernelVersion = strings.ToLower(strings.TrimSpace(string(out)))
		}
	}

	platform, family, pver, err := PlatformInformation()
	if err == nil {
		ret.Platform = platform
		ret.PlatformFamily = family
		ret.PlatformVersion = pver
	}

	system, role, err := Virtualization()
	if err == nil {
		ret.VirtualizationSystem = system
		ret.VirtualizationRole = role
	}

	boot, err := BootTime()
	if err == nil {
		ret.BootTime = boot
		ret.Uptime = uptime(boot)
	}

	procs, err := process.Pids()
	if err == nil {
		ret.Procs = uint64(len(procs))
	}

	values, err := common.DoSysctrlWithContext(ctx, "kern.uuid")
	if err == nil && len(values) == 1 && values[0] != "" {
		ret.HostID = strings.ToLower(values[0])
	}

	return ret, nil
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
	values, err := common.DoSysctrlWithContext(ctx, "kern.boottime")
	if err != nil {
		return 0, err
	}
	// ex: { sec = 1392261637, usec = 627534 } Thu Feb 13 12:20:37 2014
	v := strings.Replace(values[2], ",", "", 1)
	boottime, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}
	t = uint64(boottime)
	atomic.StoreUint64(&cachedBootTime, t)

	return t, nil
}

func uptime(boot uint64) uint64 {
	return uint64(time.Now().Unix()) - boot
}

func Uptime() (uint64, error) {
	return UptimeWithContext(context.Background())
}

func UptimeWithContext(ctx context.Context) (uint64, error) {
	boot, err := BootTime()
	if err != nil {
		return 0, err
	}
	return uptime(boot), nil
}

func Users() ([]UserStat, error) {
	return UsersWithContext(context.Background())
}

func UsersWithContext(ctx context.Context) ([]UserStat, error) {
	utmpfile := "/var/run/utmpx"
	var ret []UserStat

	file, err := os.Open(utmpfile)
	if err != nil {
		return ret, err
	}
	defer file.Close()

	buf, err := ioutil.ReadAll(file)
	if err != nil {
		return ret, err
	}

	u := Utmpx{}
	entrySize := int(unsafe.Sizeof(u))
	count := len(buf) / entrySize

	for i := 0; i < count; i++ {
		b := buf[i*entrySize : i*entrySize+entrySize]

		var u Utmpx
		br := bytes.NewReader(b)
		err := binary.Read(br, binary.LittleEndian, &u)
		if err != nil {
			continue
		}
		if u.Type != USER_PROCESS {
			continue
		}
		user := UserStat{
			User:     common.IntToString(u.User[:]),
			Terminal: common.IntToString(u.Line[:]),
			Host:     common.IntToString(u.Host[:]),
			Started:  int(u.Tv.Sec),
		}
		ret = append(ret, user)
	}

	return ret, nil

}

func PlatformInformation() (string, string, string, error) {
	return PlatformInformationWithContext(context.Background())
}

func PlatformInformationWithContext(ctx context.Context) (string, string, string, error) {
	platform := ""
	family := ""
	pver := ""

	sw_vers, err := exec.LookPath("sw_vers")
	if err != nil {
		return "", "", "", err
	}
	uname, err := exec.LookPath("uname")
	if err != nil {
		return "", "", "", err
	}

	out, err := invoke.CommandWithContext(ctx, uname, "-s")
	if err == nil {
		platform = strings.ToLower(strings.TrimSpace(string(out)))
	}

	out, err = invoke.CommandWithContext(ctx, sw_vers, "-productVersion")
	if err == nil {
		pver = strings.ToLower(strings.TrimSpace(string(out)))
	}

	return platform, family, pver, nil
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
