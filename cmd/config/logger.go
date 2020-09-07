package config

import "context"

// Logger contains injected logger methods.
var Logger = struct {
	Info  func(msg string, data ...interface{})
	LogIf func(ctx context.Context, err error, errKind ...interface{})
}{
	// Initialized via injection.
}
