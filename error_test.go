package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestErrorWithStack(t *testing.T) {
	e := ErrorWithStack(fmt.Errorf("test"))
	if !strings.Contains(e.Stack(), "TestErrorWithStack") {
		t.Errorf("Stack doesn't contain current test function")
	}
}
