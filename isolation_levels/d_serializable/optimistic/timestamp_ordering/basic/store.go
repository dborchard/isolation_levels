package main

import (
	"fmt"
	"sync"
	"time"
)

type DataManager struct {
	VersionStore       map[string]string
	ActiveTransactions []*Transaction
	Lock               sync.RWMutex
}

func NewStore() *DataManager {
	return &DataManager{
		VersionStore: make(map[string]string),
	}
}

func (s *DataManager) BeginTxn() *Transaction {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	txn := &Transaction{
		StartTime: time.Now().UnixNano(),
		Writes:    make(map[string]string),
		Reads:     make(map[string]bool),
		IsActive:  true,
	}
	s.ActiveTransactions = append(s.ActiveTransactions, txn)
	return txn
}

func (s *DataManager) Commit(txn *Transaction) bool {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	// Check for write-write conflicts
	for _, t := range s.ActiveTransactions {
		if t != txn && t.IsActive && t.StartTime < txn.StartTime {
			for writeKey := range txn.Writes {
				if t.Writes[writeKey] != "" {
					fmt.Printf("Write-write conflict detected on key: %s\n", writeKey)
					return false
				}
			}
		}
	}

	// Apply writes
	for key, value := range txn.Writes {
		s.VersionStore[key] = value
	}
	txn.IsActive = false
	return true
}
