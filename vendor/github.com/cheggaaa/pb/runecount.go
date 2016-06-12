package pb

import (
	"regexp"
	"unicode/utf8"
)

// Finds the control character sequences (like colors)
var ctrlFinder = regexp.MustCompile("\x1b\x5b[0-9]+\x6d")

func escapeAwareRuneCountInString(s string) int {
	n := utf8.RuneCountInString(s)
	for _, sm := range ctrlFinder.FindAllString(s, -1) {
		n -= len(sm)
	}
	return n
}
