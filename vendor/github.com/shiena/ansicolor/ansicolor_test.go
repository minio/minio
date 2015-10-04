package ansicolor_test

import (
	"bytes"
	"testing"

	"github.com/shiena/ansicolor"
)

func TestNewAnsiColor1(t *testing.T) {
	inner := bytes.NewBufferString("")
	w := ansicolor.NewAnsiColorWriter(inner)
	if w == inner {
		t.Errorf("Get %#v, want %#v", w, inner)
	}
}

func TestNewAnsiColor2(t *testing.T) {
	inner := bytes.NewBufferString("")
	w1 := ansicolor.NewAnsiColorWriter(inner)
	w2 := ansicolor.NewAnsiColorWriter(w1)
	if w1 != w2 {
		t.Errorf("Get %#v, want %#v", w1, w2)
	}
}
