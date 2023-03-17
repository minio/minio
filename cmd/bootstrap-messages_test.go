package cmd

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestBootstrap(t *testing.T) {
	// Bootstrap events exceed bootstrap messages limit
	bsTracer := &bootstrapTracer{}
	for i := 0; i < bootstrapMsgsLimit+10; i++ {
		bsTracer.Record(fmt.Sprintf("msg-%d", i))
	}

	traceInfos := bsTracer.Events()
	if len(traceInfos) != bootstrapMsgsLimit {
		t.Fatalf("Expected length of events %d but got %d", bootstrapMsgsLimit, len(traceInfos))
	}

	// Simulate the case where bootstrap events were updated a day ago
	bsTracer.lastUpdate = time.Now().UTC().Add(-25 * time.Hour)
	bsTracer.DropEvents()
	if !bsTracer.Empty() {
		t.Fatalf("Expected all bootstrap events to have been dropped, but found %d events", len(bsTracer.Events()))
	}

	// Fewer than 4K bootstrap events
	for i := 0; i < 10; i++ {
		bsTracer.Record(fmt.Sprintf("msg-%d", i))
	}
	events := bsTracer.Events()
	if len(events) != 10 {
		t.Fatalf("Expected length of events %d but got %d", 10, len(events))
	}
	for i, traceInfo := range bsTracer.Events() {
		msg := fmt.Sprintf("msg-%d", i)
		if !strings.HasSuffix(traceInfo.Message, msg) {
			t.Fatalf("Expected %s but got %s", msg, traceInfo.Message)
		}
	}
}
