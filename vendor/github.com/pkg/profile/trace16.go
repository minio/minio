// +build !go1.7

package profile

import "io"

// mock trace support for Go 1.6 and earlier.

func startTrace(w io.Writer) error { return nil }
func stopTrace()                   {}
