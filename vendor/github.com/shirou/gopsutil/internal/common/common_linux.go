// +build linux

package common

import (
	"os"
	"os/exec"
	"strings"
)

func DoSysctrl(mib string) ([]string, error) {
	sysctl, err := exec.LookPath("/sbin/sysctl")
	if err != nil {
		return []string{}, err
	}
	cmd := exec.Command(sysctl, "-n", mib)
	cmd.Env = getSysctrlEnv(os.Environ())
	out, err := cmd.Output()
	if err != nil {
		return []string{}, err
	}
	v := strings.Replace(string(out), "{ ", "", 1)
	v = strings.Replace(string(v), " }", "", 1)
	values := strings.Fields(string(v))

	return values, nil
}

func NumProcs() (uint64, error) {
	f, err := os.Open(HostProc())
	if err != nil {
		return 0, err
	}
	defer f.Close()

	list, err := f.Readdirnames(-1)
	if err != nil {
		return 0, err
	}
	return uint64(len(list)), err
}
