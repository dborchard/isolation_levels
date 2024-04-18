package main

import (
	"sync"
	"time"
)

// TimestampOracle generates timestamps for transactions.
type TimestampOracle struct {
	sync.Mutex
	current int64
}

func NewTimestampOracle() *TimestampOracle {
	return &TimestampOracle{current: time.Now().UnixNano()}
}

func (o *TimestampOracle) GetTimestamp() int64 {
	o.Lock()
	defer o.Unlock()
	o.current += 100 // Increment to simulate time passing
	return o.current
}
