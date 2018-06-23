package dnsutil

import (
	"net"
	"strings"
)

// ExtractAddressFromReverse turns a standard PTR reverse record name
// into an IP address. This works for ipv4 or ipv6.
//
// 54.119.58.176.in-addr.arpa. becomes 176.58.119.54. If the conversion
// fails the empty string is returned.
func ExtractAddressFromReverse(reverseName string) string {
	search := ""

	f := reverse

	switch {
	case strings.HasSuffix(reverseName, v4arpaSuffix):
		search = strings.TrimSuffix(reverseName, v4arpaSuffix)
	case strings.HasSuffix(reverseName, v6arpaSuffix):
		search = strings.TrimSuffix(reverseName, v6arpaSuffix)
		f = reverse6
	default:
		return ""
	}

	// Reverse the segments and then combine them.
	return f(strings.Split(search, "."))
}

func reverse(slice []string) string {
	for i := 0; i < len(slice)/2; i++ {
		j := len(slice) - i - 1
		slice[i], slice[j] = slice[j], slice[i]
	}
	ip := net.ParseIP(strings.Join(slice, ".")).To4()
	if ip == nil {
		return ""
	}
	return ip.String()
}

// reverse6 reverse the segments and combine them according to RFC3596:
// b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2
// is reversed to 2001:db8::567:89ab
func reverse6(slice []string) string {
	for i := 0; i < len(slice)/2; i++ {
		j := len(slice) - i - 1
		slice[i], slice[j] = slice[j], slice[i]
	}
	slice6 := []string{}
	for i := 0; i < len(slice)/4; i++ {
		slice6 = append(slice6, strings.Join(slice[i*4:i*4+4], ""))
	}
	ip := net.ParseIP(strings.Join(slice6, ":")).To16()
	if ip == nil {
		return ""
	}
	return ip.String()
}

const (
	// v4arpaSuffix is the reverse tree suffix for v4 IP addresses.
	v4arpaSuffix = ".in-addr.arpa."
	// v6arpaSuffix is the reverse tree suffix for v6 IP addresses.
	v6arpaSuffix = ".ip6.arpa."
)
