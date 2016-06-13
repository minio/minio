// +build windows

package pb

import (
	"fmt"
	"log"
)

func (p *Pool) print(first bool) bool {
	var out string
	if !first {
		coords, err := getCursorPos()
		if err != nil {
			log.Panic(err)
		}
		coords.Y -= int16(len(p.bars))
		coords.X = 0

		err = setCursorPos(coords)
		if err != nil {
			log.Panic(err)
		}
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
