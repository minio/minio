package mem

import (
	"fmt"
)

const (
	BYTES     = 1
	KILOBYTES = BYTES << 10
	MEGABYTES = KILOBYTES << 10
	GIGABYTES = MEGABYTES << 10
	TERABYTES = GIGABYTES << 10
)

func fmtBytes(bytes uint64) string {
	unit := ""
	value := float64(0)
	if bytes >= TERABYTES {
		unit = "T"
		value = float64(bytes) / float64(TERABYTES)
	} else if bytes >= GIGABYTES {
		unit = "G"
		value = float64(bytes) / float64(GIGABYTES)
	} else if bytes >= MEGABYTES {
		unit = "M"
		value = float64(bytes) / float64(MEGABYTES)
	} else if bytes >= KILOBYTES {
		unit = "K"
		value = float64(bytes) / float64(KILOBYTES)
	}
	return fmt.Sprintf("%.2f%s", value, unit)
}
