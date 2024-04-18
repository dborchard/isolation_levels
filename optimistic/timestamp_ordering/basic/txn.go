package main

import (
	"fmt"
)

type Transaction struct {
	StartTime int64
	Writes    map[string]string
	Reads     map[string]bool
	IsActive  bool
	store     *DataManager
}

func (txn *Transaction) Read(key string) (string, bool) {
	if !txn.IsActive {
		return "", false
	}

	txn.Reads[key] = true
	value, exists := txn.Writes[key]
	if exists {
		return value, true
	}

	txn.store.Lock.RLock()
	defer txn.store.Lock.RUnlock()
	value, exists = txn.store.VersionStore[key]
	return value, exists
}

func (txn *Transaction) Write(key string, value string) {
	if !txn.IsActive {
		return
	}
	txn.Writes[key] = value
}

func main() {
	store := NewStore()

	txn1 := store.BeginTxn()
	txn1.Write("a", "apple")
	fmt.Println("Transaction 1 Write: a -> apple")

	txn2 := store.BeginTxn()
	txn2.Write("a", "banana")
	fmt.Println("Transaction 2 Write: a -> banana")

	if store.Commit(txn1) {
		fmt.Println("Transaction 1 Committed")
	} else {
		fmt.Println("Transaction 1 Commit Failed")
	}

	if store.Commit(txn2) {
		fmt.Println("Transaction 2 Committed")
	} else {
		fmt.Println("Transaction 2 Commit Failed")
	}
}
