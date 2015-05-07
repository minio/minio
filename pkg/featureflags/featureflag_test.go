package featureflags

import (
	"testing"
)

func TestFeatureFlag(t *testing.T) {
	foo := Get("foo")
	if foo {
		t.Fail()
	}
	Enable("foo")
	foo = Get("foo")
	if !foo {
		t.Fail()
	}
	Disable("foo")
	foo = Get("foo")
	if foo {
		t.Fail()
	}
}
