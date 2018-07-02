// +build linux freebsd darwin openbsd

package common

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
)

func CallLsofWithContext(ctx context.Context, invoke Invoker, pid int32, args ...string) ([]string, error) {
	var cmd []string
	if pid == 0 { // will get from all processes.
		cmd = []string{"-a", "-n", "-P"}
	} else {
		cmd = []string{"-a", "-n", "-P", "-p", strconv.Itoa(int(pid))}
	}
	cmd = append(cmd, args...)
	lsof, err := exec.LookPath("lsof")
	if err != nil {
		return []string{}, err
	}
	out, err := invoke.CommandWithContext(ctx, lsof, cmd...)
	if err != nil {
		// if no pid found, lsof returnes code 1.
		if err.Error() == "exit status 1" && len(out) == 0 {
			return []string{}, nil
		}
	}
	lines := strings.Split(string(out), "\n")

	var ret []string
	for _, l := range lines[1:] {
		if len(l) == 0 {
			continue
		}
		ret = append(ret, l)
	}
	return ret, nil
}

func CallPgrepWithContext(ctx context.Context, invoke Invoker, pid int32) ([]int32, error) {
	var cmd []string
	cmd = []string{"-P", strconv.Itoa(int(pid))}
	pgrep, err := exec.LookPath("pgrep")
	if err != nil {
		return []int32{}, err
	}
	out, err := invoke.CommandWithContext(ctx, pgrep, cmd...)
	if err != nil {
		return []int32{}, err
	}
	lines := strings.Split(string(out), "\n")
	ret := make([]int32, 0, len(lines))
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		i, err := strconv.Atoi(l)
		if err != nil {
			continue
		}
		ret = append(ret, int32(i))
	}
	return ret, nil
}
