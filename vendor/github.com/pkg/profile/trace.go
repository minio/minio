// +build go1.7

package profile

import "runtime/trace"

var startTrace = trace.Start
var stopTrace = trace.Stop
