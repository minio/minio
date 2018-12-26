// +build lz4debug

package lz4

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const debugFlag = true

func debug(args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	file = filepath.Base(file)

	f := fmt.Sprintf("LZ4: %s:%d %s", file, line, args[0])
	if f[len(f)-1] != '\n' {
		f += "\n"
	}
	fmt.Fprintf(os.Stderr, f, args[1:]...)
}
