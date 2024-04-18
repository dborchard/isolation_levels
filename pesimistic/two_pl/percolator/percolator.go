package main

import (
	"fmt"
	"isolation_levels/optimistic/ssi"
	"sync"
)

// DataManager simulates a distributed key-value store.
type DataManager struct {
	sync.Mutex
	VersionStore map[string][]ssi.DataVersion
	oracle       *TimestampOracle
}

func NewDataStore(oracle *TimestampOracle) *DataManager {
	return &DataManager{
		VersionStore: make(map[string][]ssi.DataVersion),
		oracle:       oracle,
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

	// Check the latest version for a lock
	if versions, exists := store.VersionStore[key]; exists {
		if len(versions) > 0 {
			latestVersion := versions[len(versions)-1]
			if latestVersion.Lock != 0 {
				return fmt.Errorf("prewrite failed: %s is locked", key)
			}
		}
	}

	// Lock the row and write data with the start timestamp
	store.VersionStore[key] = append(store.VersionStore[key], ssi.DataVersion{
		Data: value,
		Lock: startTS,
	})
	return nil
}

// Commit attempts to unlock the row and write the commit record.
func (store *DataManager) Commit(key string, startTS int64) error {
	store.Lock()
	defer store.Unlock()

	versions := store.VersionStore[key]
	if len(versions) == 0 {
		return fmt.Errorf("commit failed: no versions found for key %s", key)
	}

	latestVersion := versions[len(versions)-1]
	if latestVersion.Lock != startTS {
		return fmt.Errorf("commit failed: lock %s not found or mismatch", key)
	}

	commitTS := store.oracle.GetTimestamp()
	// Unlock the row and record the commit timestamp
	latestVersion.Lock = 0
	latestVersion.Write = commitTS
	store.VersionStore[key][len(versions)-1] = latestVersion
	return nil
}

// Get reads the value only if there is no lock and the read timestamp is greater than the last write.
func (store *DataManager) Get(key string, readTS int64) (string, error) {
	store.Lock()
	defer store.Unlock()

	versions := store.VersionStore[key]
	if len(versions) == 0 {
		return "", fmt.Errorf("key %s not found", key)
	}

	// Return the latest version that is visible to the transaction
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if v.Write == 0 || v.Write > readTS {
			continue
		}
		if v.Lock != 0 && v.Lock != readTS {
			continue
		}
		return v.Data, nil
	}

	return "", fmt.Errorf("no visible version found for key %s", key)
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
