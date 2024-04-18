package main

import (
	"fmt"
	"isolation_levels/optimistic/ssi"
	"sync"
	"time"
)

type Transaction struct {
	ID          int
	StartTime   time.Time
	InConflict  bool
	OutConflict bool
	Committed   bool
}

type DataManager struct {
	mu                 sync.Mutex
	ActiveTransactions map[int]*Transaction          // ActiveTransactions hold the list of transactions that are not yet committed or aborted
	VersionStore       map[string][]*ssi.DataVersion // VersionStore simulates the storage of different versions of data items
}

func NewDataManager() *DataManager {
	return &DataManager{
		ActiveTransactions: make(map[int]*Transaction),
		VersionStore:       make(map[string][]*ssi.DataVersion),
	}
}

func (dm *DataManager) BeginTransaction(id int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.ActiveTransactions[id] = &Transaction{
		ID:        id,
		StartTime: time.Now(),
	}
}

func (dm *DataManager) ReadTransaction(id int, key string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	trans, exists := dm.ActiveTransactions[id]
	if !exists {
		fmt.Println("Transaction does not exist")
		return
	}

	// Simulate reading the last committed version of the data at the transaction start time
	var lastVersion *ssi.DataVersion
	for _, v := range dm.VersionStore[key] {
		if v.CreatedAt.Before(trans.StartTime) && (lastVersion == nil || v.CreatedAt.After(lastVersion.CreatedAt)) {
			lastVersion = v
		}
	}

	if lastVersion != nil {
		fmt.Printf("Read by transaction %d: %s\n", id, lastVersion.Data)
		// Check for conflicts
		for _, otherTrans := range dm.ActiveTransactions {
			if otherTrans.StartTime.Before(trans.StartTime) && otherTrans.ID != id {
				trans.OutConflict = true
				otherTrans.InConflict = true
				if otherTrans.InConflict && otherTrans.OutConflict {
					dm.abortTransaction(otherTrans.ID)
				}
			}
		}
	} else {
		fmt.Println("No data found")
	}
}

func (dm *DataManager) WriteTransaction(id int, key string, data string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	trans, exists := dm.ActiveTransactions[id]
	if !exists {
		fmt.Println("Transaction does not exist")
		return
	}

	newVersion := &ssi.DataVersion{
		VersionID: len(dm.VersionStore[key]) + 1,
		Data:      data,
		CreatedBy: id,
		CreatedAt: time.Now(),
	}
	dm.VersionStore[key] = append(dm.VersionStore[key], newVersion)

	// Check for conflicts
	for _, otherTrans := range dm.ActiveTransactions {
		if otherTrans.StartTime.Before(trans.StartTime) && otherTrans.ID != id {
			otherTrans.OutConflict = true
			trans.InConflict = true
			if otherTrans.InConflict && otherTrans.OutConflict {
				dm.abortTransaction(otherTrans.ID)
			}
		}
	}

	fmt.Printf("Write by transaction %d: %s\n", id, data)
}

func (dm *DataManager) CommitTransaction(id int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	trans, exists := dm.ActiveTransactions[id]
	if !exists {
		fmt.Println("Transaction does not exist")
		return
	}

	if trans.InConflict && trans.OutConflict {
		dm.abortTransaction(id)
	} else {
		trans.Committed = true
		fmt.Printf("Transaction %d committed successfully.\n", id)
		delete(dm.ActiveTransactions, id)
	}
}

func (dm *DataManager) abortTransaction(id int) {
	fmt.Printf("Transaction %d aborted due to conflict.\n", id)
	delete(dm.ActiveTransactions, id)
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
