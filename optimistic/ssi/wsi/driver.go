package main

import (
	"fmt"
	"isolation_levels/optimistic/ssi"
	"sync"
	"time"
)

// Transaction represents a transaction with read and write sets.
type Transaction struct {
	ID         int
	StartTime  time.Time
	CommitTime time.Time
	ReadSet    map[string]bool
	WriteSet   map[string]string
	Status     string
}

// DataManager represents a multi-version key-value store.
type DataManager struct {
	sync.RWMutex
	LastCommit   map[string]time.Time         // Last commit time for each key to detect conflicts
	VersionStore map[string][]ssi.DataVersion // Map of key to a slice of data versions
}

func NewDataManager() *DataManager {
	return &DataManager{
		VersionStore: make(map[string][]ssi.DataVersion),
		LastCommit:   make(map[string]time.Time),
	}
}

func (store *DataManager) ReadTransaction(tx *Transaction, key string) string {
	store.RLock()
	defer store.RUnlock()

	versions, exists := store.VersionStore[key]
	if !exists {
		return "" // No data exists for the key
	}

	var value string
	// Find the most recent version before the transaction's start time
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].CreatedAt.Before(tx.StartTime) {
			value = versions[i].Data
			break
		}
	}
	tx.ReadSet[key] = true // Mark this key as read
	return value
}

func (store *DataManager) WriteTransaction(tx *Transaction, key, value string) {
	tx.WriteSet[key] = value
}

func (store *DataManager) CommitTransaction(tx *Transaction) bool {
	store.Lock()
	defer store.Unlock()

	// First, check for read-write conflicts
	for key := range tx.ReadSet {
		if commitTime, ok := store.LastCommit[key]; ok && commitTime.After(tx.StartTime) {
			tx.Status = "aborted"
			return false
		}
	}

	// No conflicts, apply writes
	commitTime := time.Now()
	for key, value := range tx.WriteSet {
		versions := store.VersionStore[key]
		newVersion := ssi.DataVersion{Data: value, CreatedAt: commitTime}
		store.VersionStore[key] = append(versions, newVersion)
		store.LastCommit[key] = commitTime
	}

	tx.CommitTime = commitTime
	tx.Status = "committed"
	return true
}

func main() {
	store := NewDataManager()

	tx1 := &Transaction{
		ID:        1,
		StartTime: time.Now(),
		ReadSet:   make(map[string]bool),
		WriteSet:  make(map[string]string),
		Status:    "active",
	}

	tx2 := &Transaction{
		ID:        2,
		StartTime: time.Now().Add(time.Second), // Start a second later
		ReadSet:   make(map[string]bool),
		WriteSet:  make(map[string]string),
		Status:    "active",
	}

	// Transaction 1 writes to key "a"
	store.WriteTransaction(tx1, "a", "Data from T1")
	// Transaction 2 writes to key "a"
	store.WriteTransaction(tx2, "a", "Data from T2")

	// Both transactions try to commit
	committed1 := store.CommitTransaction(tx1)
	committed2 := store.CommitTransaction(tx2)

	fmt.Printf("Transaction 1 committed: %v\n", committed1)
	fmt.Printf("Transaction 2 committed: %v\n", committed2)
}
