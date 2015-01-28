package tdlog

import (
	"testing"
)

func TestAllLogLevels(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Did not catch Panicf")
		}
	}()
	Infof("a %s %s", "b", "c")
	Info("a", "b", "c")
	Noticef("a %s %s", "b", "c")
	Notice("a", "b", "c")
	CritNoRepeat("a %s %s", "b", "c")
	if _, exists := critHistory["a b c"]; !exists {
		t.Fatal("did not record history")
	}
	Panicf("a %s %s", "b", "c")
	t.Fatal("Cannot reach here")
}
