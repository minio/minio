package dnsutil

import (
	"fmt"
	"net"
	"os"

	"github.com/miekg/dns"
)

// ParseHostPortOrFile parses the strings in s, each string can either be a address,
// address:port or a filename. The address part is checked and the filename case a
// resolv.conf like file is parsed and the nameserver found are returned.
func ParseHostPortOrFile(s ...string) ([]string, error) {
	var servers []string
	for _, host := range s {
		addr, _, err := net.SplitHostPort(host)
		if err != nil {
			// Parse didn't work, it is not a addr:port combo
			if net.ParseIP(host) == nil {
				// Not an IP address.
				ss, err := tryFile(host)
				if err == nil {
					servers = append(servers, ss...)
					continue
				}
				return servers, fmt.Errorf("not an IP address or file: %q", host)
			}
			ss := net.JoinHostPort(host, "53")
			servers = append(servers, ss)
			continue
		}

		if net.ParseIP(addr) == nil {
			// No an IP address.
			ss, err := tryFile(host)
			if err == nil {
				servers = append(servers, ss...)
				continue
			}
			return servers, fmt.Errorf("not an IP address or file: %q", host)
		}
		servers = append(servers, host)
	}
	return servers, nil
}

// Try to open this is a file first.
func tryFile(s string) ([]string, error) {
	c, err := dns.ClientConfigFromFile(s)
	if err == os.ErrNotExist {
		return nil, fmt.Errorf("failed to open file %q: %q", s, err)
	} else if err != nil {
		return nil, err
	}

	servers := []string{}
	for _, s := range c.Servers {
		servers = append(servers, net.JoinHostPort(s, c.Port))
	}
	return servers, nil
}

// ParseHostPort will check if the host part is a valid IP address, if the
// IP address is valid, but no port is found, defaultPort is added.
func ParseHostPort(s, defaultPort string) (string, error) {
	addr, port, err := net.SplitHostPort(s)
	if port == "" {
		port = defaultPort
	}
	if err != nil {
		if net.ParseIP(s) == nil {
			return "", fmt.Errorf("must specify an IP address: `%s'", s)
		}
		return net.JoinHostPort(s, port), nil
	}

	if net.ParseIP(addr) == nil {
		return "", fmt.Errorf("must specify an IP address: `%s'", addr)
	}
	return net.JoinHostPort(addr, port), nil
}
