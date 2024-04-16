package lifecycle

import (
	"encoding/xml"
	"fmt"
	"testing"
)

func TestDelMarkerExpParseAndValidate(t *testing.T) {
	tests := []struct {
		xml string
		err error
	}{
		{
			xml: `<DelMarkerExpiration> <Days> 1 </Days> </DelMarkerExpiration>`,
			err: nil,
		},
		{
			xml: `<DelMarkerExpiration> <Days> -1 </Days> </DelMarkerExpiration>`,
			err: errInvalidDaysDelMarkerExpiration,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("TestDelMarker-%d", i), func(t *testing.T) {
			var dexp DelMarkerExpiration
			var fail bool
			err := xml.Unmarshal([]byte(test.xml), &dexp)
			if test.err == nil {
				if err != nil {
					fail = true
				}
			} else {
				if err == nil {
					fail = true
				}
				if test.err.Error() != err.Error() {
					fail = true
				}
			}
			if fail {
				t.Fatalf("Expected %v but got %v", test.err, err)
			}
		})
	}
}
