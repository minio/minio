// Package stats defines a lightweight interface for collecting statistics. It
// doesn't provide an implementation, just the shared interface.
package stats

// Client provides methods to collection statistics.
type Client interface {
	// BumpAvg bumps the average for the given key.
	BumpAvg(key string, val float64)

	// BumpSum bumps the sum for the given key.
	BumpSum(key string, val float64)

	// BumpHistogram bumps the histogram for the given key.
	BumpHistogram(key string, val float64)

	// BumpTime is a special version of BumpHistogram which is specialized for
	// timers. Calling it starts the timer, and it returns a value on which End()
	// can be called to indicate finishing the timer. A convenient way of
	// recording the duration of a function is calling it like such at the top of
	// the function:
	//
	//     defer s.BumpTime("my.function").End()
	BumpTime(key string) interface {
		End()
	}
}

// PrefixClient adds multiple keys for the same value, with each prefix
// added to the key and calls the underlying client.
func PrefixClient(prefixes []string, client Client) Client {
	return &prefixClient{
		Prefixes: prefixes,
		Client:   client,
	}
}

type prefixClient struct {
	Prefixes []string
	Client   Client
}

func (p *prefixClient) BumpAvg(key string, val float64) {
	for _, prefix := range p.Prefixes {
		p.Client.BumpAvg(prefix+key, val)
	}
}

func (p *prefixClient) BumpSum(key string, val float64) {
	for _, prefix := range p.Prefixes {
		p.Client.BumpSum(prefix+key, val)
	}
}

func (p *prefixClient) BumpHistogram(key string, val float64) {
	for _, prefix := range p.Prefixes {
		p.Client.BumpHistogram(prefix+key, val)
	}
}

func (p *prefixClient) BumpTime(key string) interface {
	End()
} {
	var m multiEnder
	for _, prefix := range p.Prefixes {
		m = append(m, p.Client.BumpTime(prefix+key))
	}
	return m
}

// multiEnder combines many enders together.
type multiEnder []interface {
	End()
}

func (m multiEnder) End() {
	for _, e := range m {
		e.End()
	}
}

// HookClient is useful for testing. It provides optional hooks for each
// expected method in the interface, which if provided will be called. If a
// hook is not provided, it will be ignored.
type HookClient struct {
	BumpAvgHook       func(key string, val float64)
	BumpSumHook       func(key string, val float64)
	BumpHistogramHook func(key string, val float64)
	BumpTimeHook      func(key string) interface {
		End()
	}
}

// BumpAvg will call BumpAvgHook if defined.
func (c *HookClient) BumpAvg(key string, val float64) {
	if c.BumpAvgHook != nil {
		c.BumpAvgHook(key, val)
	}
}

// BumpSum will call BumpSumHook if defined.
func (c *HookClient) BumpSum(key string, val float64) {
	if c.BumpSumHook != nil {
		c.BumpSumHook(key, val)
	}
}

// BumpHistogram will call BumpHistogramHook if defined.
func (c *HookClient) BumpHistogram(key string, val float64) {
	if c.BumpHistogramHook != nil {
		c.BumpHistogramHook(key, val)
	}
}

// BumpTime will call BumpTimeHook if defined.
func (c *HookClient) BumpTime(key string) interface {
	End()
} {
	if c.BumpTimeHook != nil {
		return c.BumpTimeHook(key)
	}
	return NoOpEnd
}

type noOpEnd struct{}

func (n noOpEnd) End() {}

// NoOpEnd provides a dummy value for use in tests as valid return value for
// BumpTime().
var NoOpEnd = noOpEnd{}

// BumpAvg calls BumpAvg on the Client if it isn't nil. This is useful when a
// component has an optional stats.Client.
func BumpAvg(c Client, key string, val float64) {
	if c != nil {
		c.BumpAvg(key, val)
	}
}

// BumpSum calls BumpSum on the Client if it isn't nil. This is useful when a
// component has an optional stats.Client.
func BumpSum(c Client, key string, val float64) {
	if c != nil {
		c.BumpSum(key, val)
	}
}

// BumpHistogram calls BumpHistogram on the Client if it isn't nil. This is
// useful when a component has an optional stats.Client.
func BumpHistogram(c Client, key string, val float64) {
	if c != nil {
		c.BumpHistogram(key, val)
	}
}

// BumpTime calls BumpTime on the Client if it isn't nil. If the Client is nil
// it still returns a valid return value which will be a no-op. This is useful
// when a component has an optional stats.Client.
func BumpTime(c Client, key string) interface {
	End()
} {
	if c != nil {
		return c.BumpTime(key)
	}
	return NoOpEnd
}
