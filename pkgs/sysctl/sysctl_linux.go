// !build linux,amd64

package sysctl

import (
	"bufio"
	"os/exec"
	"strings"

	"github.com/minio-io/minio/pkgs/utils"
)

type Sysctl struct {
	Sysattrmap map[string][]byte
}

func (s *Sysctl) Get() error {
	attrMap := make(map[string][]byte)

	// Get full system level list
	sysctl_cmd := exec.Command("sysctl", "-a")
	// Sort the output and throw away duplicate keys
	sort_cmd := exec.Command("sort", "-u")
	// Ignore permission denied errors from sysctl output
	grep_cmd := exec.Command("grep", "-v", "permission")

	output, err := utils.ExecPipe(sysctl_cmd, sort_cmd, grep_cmd)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(output)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanLines(data, atEOF)
		return
	}

	scanner.Split(split)

	for scanner.Scan() {
		split := strings.Split(scanner.Text(), "=")
		if len(split) < 1 {
			println("Invalid token skip..")
			continue
		}
		k, v := strings.TrimSpace(split[0]), strings.TrimSpace(split[1])
		attrMap[k] = []byte(v)
	}

	s.Sysattrmap = attrMap
	return nil
}
