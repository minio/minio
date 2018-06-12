package dnsutil

import (
	"errors"

	"github.com/miekg/dns"
)

// TrimZone removes the zone component from q. It returns the trimmed
// name or an error is zone is longer then qname. The trimmed name will be returned
// without a trailing dot.
func TrimZone(q string, z string) (string, error) {
	zl := dns.CountLabel(z)
	i, ok := dns.PrevLabel(q, zl)
	if ok || i-1 < 0 {
		return "", errors.New("trimzone: overshot qname: " + q + "for zone " + z)
	}
	// This includes the '.', remove on return
	return q[:i-1], nil
}
