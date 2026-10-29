package demo

import (
	"net"
	"os"
	"strings"
)

// TrustedPeer permits only actual loopback peers or explicitly enabled private
// container subnets. Forwarded headers never authorize a demo request.
func TrustedPeer(remote string) bool {
	host, _, e := net.SplitHostPort(remote)
	if e != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if os.Getenv("DEMO_ALLOW_CONTAINER_LISTEN") != "true" || !ip.IsPrivate() {
		return false
	}
	matched := false
	for _, raw := range strings.Split(os.Getenv("DEMO_TRUSTED_CIDRS"), ",") {
		networkIP, network, e := net.ParseCIDR(strings.TrimSpace(raw))
		if e != nil || !networkIP.IsPrivate() {
			return false
		}
		ones, bits := network.Mask.Size()
		min := 7
		if bits == 32 {
			min = 16
			if networkIP[12] == 10 {
				min = 8
			} else if networkIP[12] == 172 {
				min = 12
			}
		}
		if ones < min {
			return false
		}
		if network.Contains(ip) {
			matched = true
		}
	}
	return matched
}
