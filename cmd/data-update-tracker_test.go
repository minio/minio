package cmd

import (
	"bytes"
	"testing"

	"github.com/willf/bloom"
)

func Test_initDataUsageTracker(t *testing.T) {
	bf := bloom.NewWithEstimates(dataUpdateTrackerEstItems, dataUpdateTrackerFP)
	t.Log("m:", bf.Cap(), "k:", bf.K())
	var buf bytes.Buffer
	bf.WriteTo(&buf)
	t.Log(buf.Len())
}
