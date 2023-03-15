package cmd

import "testing"

func TestAllBootstrapMessages(t *testing.T) {
	for i := bootstrapEvent(0); i < bsLastEvent; i++ {
		if len(bootstrapMsgs[i]) <= 0 {
			t.Fatalf("bootstrap message for event %d is empty", i)
		}
	}
}
