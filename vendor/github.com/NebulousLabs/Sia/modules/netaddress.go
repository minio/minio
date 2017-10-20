package modules

import (
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/NebulousLabs/Sia/build"
)

// MaxEncodedNetAddressLength is the maximum length of a NetAddress encoded
// with the encode package. 266 was chosen because the maximum length for the
// hostname is 254 + 1 for the separating colon + 5 for the port + 8 byte
// string length prefix.
const MaxEncodedNetAddressLength = 266

// A NetAddress contains the information needed to contact a peer.
type NetAddress string

// Host removes the port from a NetAddress, returning just the host. If the
// address is not of the form "host:port" the empty string is returned. The
// port will still be returned for invalid NetAddresses (e.g. "unqualified:0"
// will return "unqualified"), but in general you should only call Host on
// valid addresses.
func (na NetAddress) Host() string {
	host, _, err := net.SplitHostPort(string(na))
	// 'host' is not always the empty string if an error is returned.
	if err != nil {
		return ""
	}
	return host
}

// Port returns the NetAddress object's port number. If the address is not of
// the form "host:port" the empty string is returned. The port will still be
// returned for invalid NetAddresses (e.g. "localhost:0" will return "0"), but
// in general you should only call Port on valid addresses.
func (na NetAddress) Port() string {
	_, port, err := net.SplitHostPort(string(na))
	// 'port' will not always be the empty string if an error is returned.
	if err != nil {
		return ""
	}
	return port
}

// IsLoopback returns true for IP addresses that are on the same machine.
func (na NetAddress) IsLoopback() bool {
	host, _, err := net.SplitHostPort(string(na))
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

// IsLocal returns true if the input IP address belongs to a local address
// range such as 192.168.x.x or 127.x.x.x
func (na NetAddress) IsLocal() bool {
	// Loopback counts as private.
	if na.IsLoopback() {
		return true
	}

	// Grab the IP address of the net address. If there is an error parsing,
	// return false, as it's not a private ip address range.
	ip := net.ParseIP(na.Host())
	if ip == nil {
		return false
	}

	// Determine whether or not the ip is in a CIDR that is considered to be
	// local.
	localCIDRs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fd00::/8",
	}
	for _, cidr := range localCIDRs {
		_, ipnet, _ := net.ParseCIDR(cidr)
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

// IsValid is an extension to IsStdValid that also forbids the loopback
// address. IsValid is being phased out in favor of allowing the loopback
// address but verifying through other means that the connection is not to
// yourself (which is the original reason that the loopback address was
// banned).
func (na NetAddress) IsValid() error {
	// Check the loopback address.
	if na.IsLoopback() && build.Release != "testing" {
		return errors.New("host is a loopback address")
	}
	return na.IsStdValid()
}

// IsStdValid returns an error if the NetAddress is invalid. A valid NetAddress
// is of the form "host:port", such that "host" is either a valid IPv4/IPv6
// address or a valid hostname, and "port" is an integer in the range
// [1,65535]. Valid IPv4 addresses, IPv6 addresses, and hostnames are detailed
// in RFCs 791, 2460, and 952, respectively.
func (na NetAddress) IsStdValid() error {
	// Verify the port number.
	host, port, err := net.SplitHostPort(string(na))
	if err != nil {
		return err
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return errors.New("port is not an integer")
	} else if portInt < 1 || portInt > 65535 {
		return errors.New("port is invalid")
	}

	// Loopback addresses don't always pass the requirements below, and
	// therefore must be checked separately.
	if na.IsLoopback() {
		return nil
	}

	// First try to parse host as an IP address; if that fails, assume it is a
	// hostname.
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() {
			return errors.New("host is the unspecified address")
		}
	} else {
		// Hostnames can have a trailing dot (which indicates that the hostname is
		// fully qualified), but we ignore it for validation purposes.
		if strings.HasSuffix(host, ".") {
			host = host[:len(host)-1]
		}
		if len(host) < 1 || len(host) > 253 {
			return errors.New("invalid hostname length")
		}
		labels := strings.Split(host, ".")
		if len(labels) == 1 {
			return errors.New("unqualified hostname")
		}
		for _, label := range labels {
			if len(label) < 1 || len(label) > 63 {
				return errors.New("hostname contains label with invalid length")
			}
			if strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
				return errors.New("hostname contains label that starts or ends with a hyphen")
			}
			for _, r := range strings.ToLower(label) {
				isLetter := 'a' <= r && r <= 'z'
				isNumber := '0' <= r && r <= '9'
				isHyphen := r == '-'
				if !(isLetter || isNumber || isHyphen) {
					return errors.New("host contains invalid characters")
				}
			}
		}
	}

	return nil
}
