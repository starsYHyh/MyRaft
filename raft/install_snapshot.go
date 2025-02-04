package raft

import (
	"sync"
	"time"
)

type SnapshotController struct {
	wg          sync.WaitGroup
	appendCount int
	appendCh    chan bool
	timeout     <-chan time.Time
}
