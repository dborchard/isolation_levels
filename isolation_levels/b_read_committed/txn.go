package main

import (
	"sync"
	"time"
)

type DataVersion struct {
	Value     string
	Timestamp int64
	Committed bool
}

type DataManager struct {
	Data map[string]*DataVersion
	Lock sync.RWMutex
}

type Transaction struct {
	StartTime   int64
	DataManager *DataManager
}

func NewDataManager() *DataManager {
	return &DataManager{
		Data: make(map[string]*DataVersion),
	}
}

func (dm *DataManager) BeginTxn() *Transaction {
	return &Transaction{
		StartTime:   time.Now().UnixNano(),
		DataManager: dm,
	}
}

func (txn *Transaction) Read(key string) (string, bool) {
	txn.DataManager.Lock.RLock()
	defer txn.DataManager.Lock.RUnlock()

	if record, exists := txn.DataManager.Data[key]; exists && record.Committed {
		return record.Value, true
	}
	return "", false
}

func (txn *Transaction) Write(key string, value string) {
	currentTime := time.Now().UnixNano()
	txn.DataManager.Lock.Lock()
	defer txn.DataManager.Lock.Unlock()

	// Write the data as uncommitted initially
	txn.DataManager.Data[key] = &DataVersion{
		Value:     value,
		Timestamp: currentTime,
		Committed: false,
	}
}

func (txn *Transaction) Commit() {
	txn.DataManager.Lock.Lock()
	defer txn.DataManager.Lock.Unlock()

	// Commit all writes made by the transaction
	for _, record := range txn.DataManager.Data {
		if record.Timestamp >= txn.StartTime {
			record.Committed = true
		}
	}
}

func main() {
	dm := NewDataManager()

	// Transaction 1: Writes data
	txn1 := dm.BeginTxn()
	txn1.Write("a", "Hello, World!")
	txn1.Commit()

	// Transaction 2: Attempts to read the data written by Transaction 1
	txn2 := dm.BeginTxn()
	if value, exists := txn2.Read("a"); exists {
		println("Transaction 2 read: ", value)
	} else {
		println("Transaction 2 read failed to find 'a'")
	}
}
