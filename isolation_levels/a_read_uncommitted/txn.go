package main

import (
	"sync"
	"time"
)

// DataVersion represents a data item in the database.
type DataVersion struct {
	Value     string
	Timestamp int64
}

// DataManager handles the data and transactions.
type DataManager struct {
	Lock sync.RWMutex
	Data map[string]*DataVersion
}

// Transaction represents a database transaction.
type Transaction struct {
	StartTime   int64
	DataManager *DataManager
}

// NewDataManager initializes a new DataManager.
func NewDataManager() *DataManager {
	return &DataManager{
		Data: make(map[string]*DataVersion),
	}
}

// BeginTxn starts a new transaction.
func (dm *DataManager) BeginTxn() *Transaction {
	return &Transaction{
		StartTime:   time.Now().UnixNano(),
		DataManager: dm,
	}
}

// Read attempts to read a value from the database.
func (txn *Transaction) Read(key string) (string, bool) {
	txn.DataManager.Lock.RLock()
	defer txn.DataManager.Lock.RUnlock()

	if record, exists := txn.DataManager.Data[key]; exists {
		return record.Value, true
	}
	return "", false
}

// Write updates or adds a new record to the database.
func (txn *Transaction) Write(key string, value string) {
	currentTime := time.Now().UnixNano()
	txn.DataManager.Lock.Lock()
	defer txn.DataManager.Lock.Unlock()

	// Writes are immediately visible to all.
	txn.DataManager.Data[key] = &DataVersion{
		Value:     value,
		Timestamp: currentTime,
	}
}

// Commit completes the transaction. In Read Uncommitted, this is mostly formal.
func (txn *Transaction) Commit() {
	// Commit operation is simplified as all writes are immediately visible.
}

// main is the entry point of the program.
func main() {
	dm := NewDataManager()

	// Transaction 1: Writes data.
	txn1 := dm.BeginTxn()
	txn1.Write("a", "Hello, World!")
	txn1.Commit()

	// Transaction 2: Reads data written by Transaction 1.
	txn2 := dm.BeginTxn()
	if value, exists := txn2.Read("a"); exists {
		println("Transaction 2 read: ", value)
	} else {
		println("Transaction 2 read failed to find 'a'")
	}
}
