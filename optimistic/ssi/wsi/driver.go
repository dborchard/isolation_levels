package main

import (
	"fmt"
	"sync"
	"time"
)

type Transaction struct {
	ID        int
	StartTime time.Time
	Status    string
	ReadSet   map[string]string
	WriteSet  map[string]string
}

type DataManager struct {
	sync.Mutex
	Transactions       map[int]*Transaction
	LastCommit         map[string]time.Time
	TimestampGenerator int
}

func NewDataManager() *DataManager {
	return &DataManager{
		Transactions: make(map[int]*Transaction),
		LastCommit:   make(map[string]time.Time),
	}
}

func (dm *DataManager) BeginTransaction(id int) {
	dm.Lock()
	defer dm.Unlock()

	dm.Transactions[id] = &Transaction{
		ID:        id,
		StartTime: time.Now(),
		Status:    "active",
		ReadSet:   make(map[string]string),
		WriteSet:  make(map[string]string),
	}

	fmt.Printf("Transaction %d started\n", id)
}

func (dm *DataManager) WriteTransaction(id int, key string, value string) {
	dm.Lock()
	defer dm.Unlock()

	if tx, ok := dm.Transactions[id]; ok && tx.Status == "active" {
		tx.WriteSet[key] = value
		fmt.Printf("Transaction %d wrote %s to %s\n", id, value, key)
	} else {
		fmt.Printf("Transaction %d not found or not active\n", id)
	}
}

func (dm *DataManager) ReadTransaction(id int, key string) string {
	dm.Lock()
	defer dm.Unlock()

	tx, ok := dm.Transactions[id]
	if !ok || tx.Status != "active" {
		fmt.Printf("Transaction %d not found or not active\n", id)
		return ""
	}

	// Check if any committed transaction has written to the key after this transaction started
	lastCommitTime, exists := dm.LastCommit[key]
	if exists && lastCommitTime.After(tx.StartTime) {
		fmt.Printf("Transaction %d aborted due to read-write conflict on key %s\n", id, key)
		tx.Status = "aborted"
		return ""
	}

	// Simulating read value
	value := "some_data" // This would actually come from a database
	tx.ReadSet[key] = value

	fmt.Printf("Transaction %d read %s from %s\n", id, value, key)
	return value
}

func (dm *DataManager) CommitTransaction(id int) {
	dm.Lock()
	defer dm.Unlock()

	tx, ok := dm.Transactions[id]
	if !ok || tx.Status != "active" {
		fmt.Printf("Transaction %d not found or not active\n", id)
		return
	}

	// Check for read-write conflicts before committing
	for key := range tx.ReadSet {
		lastCommitTime, exists := dm.LastCommit[key]
		if exists && lastCommitTime.After(tx.StartTime) {
			fmt.Printf("Transaction %d cannot commit due to read-write conflict on key %s\n", id, key)
			tx.Status = "aborted"
			return
		}
	}

	// Commit all writes
	commitTime := time.Now()
	for key, value := range tx.WriteSet {
		dm.LastCommit[key] = commitTime
		fmt.Printf("Transaction %d committed value %s to key %s\n", id, value, key)
	}
	tx.Status = "committed"
}

func main() {
	dm := NewDataManager()
	dm.BeginTransaction(1)
	dm.BeginTransaction(2)

	dm.WriteTransaction(1, "a", "Data from T1")
	dm.WriteTransaction(2, "a", "Data from T2")

	dm.ReadTransaction(1, "a")
	dm.ReadTransaction(2, "a")

	dm.CommitTransaction(1)
	dm.CommitTransaction(2)
}
