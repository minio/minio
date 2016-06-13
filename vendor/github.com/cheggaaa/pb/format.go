package pb

import (
	"fmt"
	"strings"
	"time"
)

type Units int

const (
	// U_NO are default units, they represent a simple value and are not formatted at all.
	U_NO Units = iota
	// U_BYTES units are formatted in a human readable way (b, Bb, Mb, ...)
	U_BYTES
	// U_DURATION units are formatted in a human readable way (3h14m15s)
	U_DURATION
)

func Format(i int64) *formatter {
	return &formatter{n: i}
}

type formatter struct {
	n      int64
	unit   Units
	width  int
	perSec bool
}

func (f *formatter) Value(n int64) *formatter {
	f.n = n
	return f
}

func (f *formatter) To(unit Units) *formatter {
	f.unit = unit
	return f
}

func (f *formatter) Width(width int) *formatter {
	f.width = width
	return f
}

func (f *formatter) PerSec() *formatter {
	f.perSec = true
	return f
}

func (f *formatter) String() (out string) {
	switch f.unit {
	case U_BYTES:
		out = formatBytes(f.n)
	case U_DURATION:
		d := time.Duration(f.n)
		if d > time.Hour*24 {
			out = fmt.Sprintf("%dd", d/24/time.Hour)
			d -= (d / time.Hour / 24) * (time.Hour * 24)
		}
		out = fmt.Sprintf("%s%v", out, d)
	default:
		out = fmt.Sprintf(fmt.Sprintf("%%%dd", f.width), f.n)
	}
	if f.perSec {
		out += "/s"
	}
	return
}

// Convert bytes to human readable string. Like a 2 MB, 64.2 KB, 52 B
func formatBytes(i int64) (result string) {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f TB", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f GB", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		result = fmt.Sprintf("%.02f MB", float64(i)/1024/1024)
	case i > 1024:
		result = fmt.Sprintf("%.02f KB", float64(i)/1024)
	default:
		result = fmt.Sprintf("%d B", i)
	}
	result = strings.Trim(result, " ")
	return
}
