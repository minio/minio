// +build linux darwin freebsd netbsd openbsd solaris dragonfly

package pb

import "fmt"

func (p *Pool) print(first bool) bool {
	var out string
	if !first {
		out = fmt.Sprintf("\033[%dA", len(p.bars))
	}
	isFinished := true
	for _, bar := range p.bars {
		if !bar.isFinish {
			isFinished = false
		}
		bar.Update()
		out += fmt.Sprintf("\r%s\n", bar.String())
	}
	fmt.Print(out)
	return isFinished
}
