package main

import (
	"fmt"
	"sync"
	"time"
)

type Transaction struct {
	Timestamp int64
	Writes    map[string]string
	Reads     map[string]bool
	IsActive  bool
	store     *Store
}

type Store struct {
	Data   map[string]string
	Lock   sync.RWMutex
	Txs    []*Transaction
	TxLock sync.Mutex
}

func NewStore() *Store {
	return &Store{
		Data: make(map[string]string),
	}
}

func (s *Store) BeginTxn() *Transaction {
	s.TxLock.Lock()
	defer s.TxLock.Unlock()

	txn := &Transaction{
		Timestamp: time.Now().UnixNano(),
		Writes:    make(map[string]string),
		Reads:     make(map[string]bool),
		IsActive:  true,
	}
	s.Txs = append(s.Txs, txn)
	return txn
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
	value, exists = txn.store.Data[key]
	return value, exists
}

func (txn *Transaction) Write(key string, value string) {
	if !txn.IsActive {
		return
	}
	txn.Writes[key] = value
}

func (s *Store) Commit(txn *Transaction) bool {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	// Check for write-write conflicts
	for _, t := range s.Txs {
		if t != txn && t.IsActive && t.Timestamp < txn.Timestamp {
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
		s.Data[key] = value
	}
	txn.IsActive = false
	return true
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
