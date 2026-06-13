package grpc

import (
	"fmt"
	"math/rand"
)

// newRequestID returns a short random hex string for correlating log lines.
func newRequestID() string {
	return fmt.Sprintf("%08x", rand.Uint32()) //nolint:gosec
}
