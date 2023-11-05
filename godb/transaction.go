package godb

import (
    "sync"
)

type TransactionID *int

var lock sync.Mutex
var nextTid = 0

func NewTID() TransactionID {
    lock.Lock()
    defer lock.Unlock()
	id := nextTid
	nextTid++
	return &id
}

//var tid TransactionID = NewTID()
