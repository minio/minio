// +build linux darwin windows

package sys

import "testing"

// Test get stats result.
func TestGetStats(t *testing.T) {
	stats, err := GetStats()
	if err != nil {
		t.Errorf("Tests: Expected `nil`, Got %s", err)
	}
	if stats.TotalRAM == 0 {
		t.Errorf("Tests: Expected `n > 0`, Got %d", stats.TotalRAM)
	}
}
