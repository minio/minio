package proto

import (
	"time"

	"github.com/NebulousLabs/Sia/build"
)

var (
	// connTimeout determines the number of seconds the dialer will wait
	// for a connect to complete
	connTimeout = build.Select(build.Var{
		Dev:      10 * time.Second,
		Standard: 60 * time.Second,
		Testing:  5 * time.Second,
	}).(time.Duration)
)
