package godb

import (
    "errors"
    "sync"
    "time"
    "fmt"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	// TODO: some code goes here
    numPages int
    size int
    pages map[any](*Page)
    poolLock sync.Mutex
    aliveTransactions map[TransactionID]struct{}
    transactionReadLocks map[TransactionID](map[any]struct{})
    transactionWriteLocks map[TransactionID](map[any]struct{})
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
    ret := new(BufferPool)
    ret.numPages = numPages
    ret.pages = make(map[any](*Page))
    ret.aliveTransactions = make(map[TransactionID]struct{})
    ret.transactionReadLocks = make(map[TransactionID](map[any]struct{}))
    ret.transactionWriteLocks = make(map[TransactionID](map[any]struct{}))
	return ret
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
    for _, v := range bp.pages {
        f := (*v).getFile()
        (*f).flushPage(v)
    }
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
    bp.poolLock.Lock()
    defer bp.poolLock.Unlock()

    for pageKey, _ := range bp.transactionWriteLocks[tid] {
        page, ok  := bp.pages[pageKey]
        if (ok) {
            if ((*page).isDirty()) {
                delete(bp.pages, pageKey)
            }
        }
    }

    delete(bp.aliveTransactions, tid)
    delete(bp.transactionReadLocks, tid)
    delete(bp.transactionWriteLocks, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
    bp.poolLock.Lock()
    defer bp.poolLock.Unlock()

    for _, pageKey := range bp.transactionWriteLocks[tid] {
        page, ok := bp.pages[pageKey]
        if ok {
            if ((*page).isDirty()) {
                f := (*page).getFile()
                (*f).flushPage(page)
            }
        }
    }

    delete(bp.aliveTransactions, tid)
    delete(bp.transactionReadLocks, tid)
    delete(bp.transactionWriteLocks, tid)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
    if false {
        fmt.Println("false")
    }
    bp.poolLock.Lock()
    defer bp.poolLock.Unlock()

    bp.aliveTransactions[tid] = struct{}{}
    bp.transactionReadLocks[tid] = make(map[any]struct{})
    bp.transactionWriteLocks[tid] = make(map[any]struct{})
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// TODO: some code goes here

    pageKey := file.pageKey(pageNo)

    for {
        bp.poolLock.Lock()
        bad := false
        if perm == ReadPerm {
            // check write locks
            for other_tid, _ := range bp.aliveTransactions {
                if other_tid == tid {
                    continue
                }
                writeLocks := bp.transactionWriteLocks[other_tid]
                for lock, _ := range writeLocks {
                    if (lock == pageKey) {
                        bad = true
                    }
                }
            }
        } else if perm == WritePerm {
            // check read and write locks
            for other_tid, _ := range bp.aliveTransactions {
                if other_tid == tid {
                    continue
                }
                readLocks := bp.transactionReadLocks[other_tid]
                for lock, _ := range readLocks {
                    if (lock == pageKey) {
                        bad = true
                    }
                }
                writeLocks := bp.transactionWriteLocks[other_tid]
                for lock, _ := range writeLocks {
                    if (lock == pageKey) {
                        bad = true
                    }
                }
            }
        }
        if (bad) {
            time.Sleep(10 * time.Millisecond)
            bp.poolLock.Unlock()
        } else {
            break
        }
    }

    defer bp.poolLock.Unlock()

    if perm == ReadPerm {
        bp.transactionReadLocks[tid][pageKey] = struct{}{}
    } else if perm == WritePerm {
        bp.transactionWriteLocks[tid][pageKey] = struct{}{}
    }


    v, ok := bp.pages[pageKey]
    if ok {
        return v, nil
    }

    // page is not in buffer pool
    page, err := file.readPage(pageNo)
    if err != nil {
        return nil, err
    }
    if bp.size == bp.numPages {
        pageEvicted := false
        for k, v := range bp.pages {
            if !(*v).isDirty() {
                delete(bp.pages, k)
                pageEvicted = true
                break
            }
        }
        if !pageEvicted {
            return nil, errors.New("buffer pool is full of dirty pages")
        }
    } else {
        bp.size++
    }
    bp.pages[pageKey] = page

	return page, nil
}
