package main

import (
	"fmt"
	"isolation_levels/optimistic/ssi"

	"sync"
)

// DataManager simulates a distributed key-value store.
type DataManager struct {
	sync.Mutex
	SingleVersionStore map[string]ssi.DataVersion
	oracle             *TimestampOracle
}

func NewDataStore(oracle *TimestampOracle) *DataManager {
	return &DataManager{
		SingleVersionStore: make(map[string]ssi.DataVersion),
		oracle:             oracle,
	}
}

// BeginTransaction starts a new transaction by obtaining a start timestamp.
func (store *DataManager) BeginTransaction() int64 {
	return store.oracle.GetTimestamp()
}

// Prewrite attempts to lock the row and write data using the transaction's start timestamp.
func (store *DataManager) Prewrite(key, value string, startTS int64) error {
	store.Lock()
	defer store.Unlock()

	if v, exists := store.SingleVersionStore[key]; exists && v.Lock != 0 {
		return fmt.Errorf("prewrite failed: %s is locked", key)
	}

	// Lock the row and write VersionStore with the start timestamp
	store.SingleVersionStore[key] = ssi.DataVersion{
		Data: value,
		Lock: startTS,
	}
	return nil
}

// Commit attempts to unlock the row and write the commit record.
func (store *DataManager) Commit(key string, startTS int64) error {
	store.Lock()
	defer store.Unlock()

	v, exists := store.SingleVersionStore[key]
	if !exists || v.Lock != startTS {
		return fmt.Errorf("commit failed: lock %s not found or mismatch", key)
	}

	commitTS := store.oracle.GetTimestamp()
	// Unlock the row and record the commit timestamp
	store.SingleVersionStore[key] = ssi.DataVersion{
		Data:  v.Data,
		Lock:  0,
		Write: commitTS,
	}
	return nil
}

// Get reads the value only if there is no lock and the read timestamp is greater than the last write.
func (store *DataManager) Get(key string, readTS int64) (string, error) {
	store.Lock()
	defer store.Unlock()

	v, exists := store.SingleVersionStore[key]
	if !exists {
		return "", fmt.Errorf("key %s not found", key)
	}
	if v.Lock != 0 && v.Lock != readTS {
		return "", fmt.Errorf("key %s is locked", key)
	}
	if v.Write == 0 || v.Write > readTS {
		return "", fmt.Errorf("key %s write timestamp %d is newer than read timestamp %d", key, v.Write, readTS)
	}
	return v.Data, nil
}

func main() {
	oracle := NewTimestampOracle()
	store := NewDataStore(oracle)

	startTS := store.BeginTransaction()
	if err := store.Prewrite("key1", "value1", startTS); err != nil {
		fmt.Println("Prewrite error:", err)
		return
	}

	if err := store.Commit("key1", startTS); err != nil {
		fmt.Println("Commit error:", err)
		return
	}

	value, err := store.Get("key1", store.oracle.GetTimestamp())
	if err != nil {
		fmt.Println("Get error:", err)
	} else {
		fmt.Println("Value:", value)
	}
}
