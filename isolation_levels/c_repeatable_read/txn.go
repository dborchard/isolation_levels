package main

import (
	"sync"
	"time"
)

// Record represents a data item in the database with version control.
type Record struct {
	Value   string
	Version int64
}

// DataManager handles the database operations.
type DataManager struct {
	Data map[string]*Record
	Lock sync.RWMutex
}

// Transaction represents a database transaction.
type Transaction struct {
	StartTime   int64
	Version     int64
	ReadSet     map[string]*Record
	DataManager *DataManager
}

// NewDataManager initializes a new DataManager.
func NewDataManager() *DataManager {
	return &DataManager{
		Data: make(map[string]*Record),
	}
}

// BeginTxn starts a new transaction.
func (dm *DataManager) BeginTxn() *Transaction {
	dm.Lock.Lock()
	currentTime := time.Now().UnixNano()
	dm.Lock.Unlock()
	return &Transaction{
		StartTime:   currentTime,
		Version:     currentTime,
		ReadSet:     make(map[string]*Record),
		DataManager: dm,
	}
}

func (txn *Transaction) Read(key string) (string, bool) {
	txn.DataManager.Lock.RLock()
	defer txn.DataManager.Lock.RUnlock()

	record, exists := txn.DataManager.Data[key]
	if !exists {
		return "", false
	}
	// Capture the version of the data at the first read.
	if _, ok := txn.ReadSet[key]; !ok {
		txn.ReadSet[key] = &Record{Value: record.Value, Version: record.Version}
	}
	return record.Value, true
}

func (txn *Transaction) Write(key string, value string) {
	txn.DataManager.Lock.Lock()
	defer txn.DataManager.Lock.Unlock()

	if record, exists := txn.DataManager.Data[key]; exists {
		if record.Version > txn.StartTime {
			// Simulate write conflict by not allowing the write
			return
		}
	}
	txn.DataManager.Data[key] = &Record{Value: value, Version: txn.Version}
}

func (txn *Transaction) Commit() bool {
	txn.DataManager.Lock.Lock()
	defer txn.DataManager.Lock.Unlock()

	// Check for write conflicts in the read set
	for key, rec := range txn.ReadSet {
		if currentRecord, exists := txn.DataManager.Data[key]; exists {
			if currentRecord.Version != rec.Version {
				return false // Conflict detected, transaction fails
			}
		}
	}

	// All records in the read set are unchanged, commit successful
	for key, rec := range txn.DataManager.Data {
		if _, exists := txn.ReadSet[key]; exists {
			rec.Version = txn.Version // Update the version of the data
		}
	}
	return true
}

func main() {
	dm := NewDataManager()

	// Transaction 1: Reads and writes data
	txn1 := dm.BeginTxn()
	_, _ = txn1.Read("a")            // Initial read
	txn1.Write("a", "Hello, World!") // Write data
	if txn1.Commit() {
		println("Transaction 1 committed successfully.")
	} else {
		println("Transaction 1 commit failed.")
	}

	// Transaction 2: Tries to read the same data
	txn2 := dm.BeginTxn()
	if value, exists := txn2.Read("a"); exists {
		println("Transaction 2 read: ", value)
	} else {
		println("Transaction 2 read failed to find 'a'")
	}
}
