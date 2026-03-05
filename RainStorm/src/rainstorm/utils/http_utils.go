package utils

import (
	"fmt"
	"log"
	"net"
	"rainstorm-c7/config"
	nodeid "rainstorm-c7/membership/node"
	"strconv"
	"strings"
)

// ResolveReplicaEndpoint computes the HTTP endpoint for a given replica NodeID,
// httpPort is the port (with leading ':')
func ResolveReplicaEndpoint(rp nodeid.NodeID, config config.Config) (string, error) {

	replicaEndpoint := rp.EP.EndpointToString()
	// fmt.Println("Resolving replica endpoint for:", replicaEndpoint)

	host, _, err := net.SplitHostPort(replicaEndpoint)
	if err != nil {
		log.Printf("invalid replica endpoint %q: %v", replicaEndpoint, err)
		return "", err
	}
	// fmt.Println("Host extracted:", host)

	if config.Env == "dev" {
		port := rp.EP.Port

		_, bindPortStr, err := net.SplitHostPort(config.BindAddr)
		if err != nil {
			log.Printf("invalid bind address %q: %v", config.BindAddr, err)
			return "", err
		}
		bindPort, _ := strconv.Atoi(bindPortStr)

		diff := port - uint16(bindPort)

		rainstormHTTPPort, _ := strconv.Atoi(config.RainstormHTTP[1:]) // skip leading ':'
		fmt.Println("Calculated rainstormHTTPPort:", rainstormHTTPPort)

		replicaEndpoint = fmt.Sprintf("%s:%d", host, rainstormHTTPPort+int(diff))
	} else {
		// assuming config.RainstormHTTP already includes :port or prefix
		replicaEndpoint = fmt.Sprintf("%s%s", host, config.RainstormHTTP)
	}

	// log.Printf("Resolved replica endpoint to: %s", replicaEndpoint)

	return replicaEndpoint, nil
}

// EnsureHTTPBase ensures the given address has an http:// or https:// prefix.
func EnsureHTTPBase(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	if strings.HasPrefix(addr, ":") {
		return "http://127.0.0.1" + addr
	}
	return "http://" + addr
}
