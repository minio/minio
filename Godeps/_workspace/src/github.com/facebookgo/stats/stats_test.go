package stats_test

import (
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/stats"
)

// Ensure calling End works even when a BumpTimeHook isn't provided.
func TestHookClientBumpTime(t *testing.T) {
	(&stats.HookClient{}).BumpTime("foo").End()
}

func TestPrefixClient(t *testing.T) {
	const (
		prefix1      = "prefix1"
		prefix2      = "prefix2"
		avgKey       = "avg"
		avgVal       = float64(1)
		sumKey       = "sum"
		sumVal       = float64(2)
		histogramKey = "histogram"
		histogramVal = float64(3)
		timeKey      = "time"
	)

	var keys []string
	hc := &stats.HookClient{
		BumpAvgHook: func(key string, val float64) {
			keys = append(keys, key)
			ensure.DeepEqual(t, val, avgVal)
		},
		BumpSumHook: func(key string, val float64) {
			keys = append(keys, key)
			ensure.DeepEqual(t, val, sumVal)
		},
		BumpHistogramHook: func(key string, val float64) {
			keys = append(keys, key)
			ensure.DeepEqual(t, val, histogramVal)
		},
		BumpTimeHook: func(key string) interface {
			End()
		} {
			return multiEnderTest{
				EndHook: func() {
					keys = append(keys, key)
				},
			}
		},
	}

	pc := stats.PrefixClient([]string{prefix1, prefix2}, hc)
	pc.BumpAvg(avgKey, avgVal)
	pc.BumpSum(sumKey, sumVal)
	pc.BumpHistogram(histogramKey, histogramVal)
	pc.BumpTime(timeKey).End()

	ensure.SameElements(t, keys, []string{
		prefix1 + avgKey,
		prefix1 + sumKey,
		prefix1 + histogramKey,
		prefix1 + timeKey,
		prefix2 + avgKey,
		prefix2 + sumKey,
		prefix2 + histogramKey,
		prefix2 + timeKey,
	})
}

type multiEnderTest struct {
	EndHook func()
}

func (e multiEnderTest) End() {
	e.EndHook()
}
