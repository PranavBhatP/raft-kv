package raft

import (
	"math/rand"
	"time"
)

// GetRandomElectionTimeout returns a random duration in the election window.
func GetRandomElectionTimeout() time.Duration {
	min := 300
	max := 600
	ms := rand.Intn(max-min) + min
	return time.Duration(ms) * time.Millisecond
}
