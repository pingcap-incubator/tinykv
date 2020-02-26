package latches

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"sync"
)

type Latches struct {
	// Before modifying any property of a key, the thread must have the latch for that key. `Latches` maps each latched
	// key to a WaitGroup. Threads who find a key locked should wait on that WaitGroup.
	latchMap map[string]*sync.WaitGroup
	// Mutex to guard latchMap. A thread must hold this mutex while it makes any change to latchMap.
	latchGuard sync.Mutex
	// An optional validation function, only used for testing.
	Validation func(txn *kvstore.MvccTxn, keys [][]byte)
}

// NewLatches creates a new Latches object for managing a databases latches. There should only be one such object, shared
// between all threads.
func NewLatches() *Latches {
	l := new(Latches)
	l.latchMap = make(map[string]*sync.WaitGroup)
	return l
}

// AcquireLatches tries lock all Latches specified by keys. If this succeeds, nil is returned. If any of the keys are
// locked, then AcquireLatches requires a WaitGroup which the thread can use to be woken when the lock is free.
func (l *Latches) AcquireLatches(keysToLatch [][]byte) *sync.WaitGroup {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	// Check none of the keys we want to write are locked.
	for _, key := range keysToLatch {
		if latchWg, ok := l.latchMap[string(key)]; ok {
			// Return a wait group to wait on.
			return latchWg
		}
	}

	// All Latches are available, lock them all with a new wait group.
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, key := range keysToLatch {
		l.latchMap[string(key)] = wg
	}

	return nil
}

// ReleaseLatches releases the latches for all keys in keysToUnlatch. It will wakeup any threads blocked on one of the
// latches. All keys in keysToUnlatch must have been locked together in one call to AcquireLatches.
func (l *Latches) ReleaseLatches(keysToUnlatch [][]byte) {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	first := true
	for _, key := range keysToUnlatch {
		if first {
			wg := l.latchMap[string(key)]
			wg.Done()
			first = false
		}
		delete(l.latchMap, string(key))
	}
}

// WaitForLatches attempts to lock all keys in keysToLatch using AcquireLatches. If a latch ia already locked, then =
// WaitForLatches will wait for it to become unlocked then try again. Therefore WaitForLatches may block for an unbounded
// length of time.
func (l *Latches) WaitForLatches(keysToLatch [][]byte) {
	for {
		wg := l.AcquireLatches(keysToLatch)
		if wg == nil {
			return
		}
		wg.Wait()
	}
}

// Validate calls the function in Validation, if it exists.
func (l *Latches) Validate(txn *kvstore.MvccTxn, latched [][]byte) {
	if l.Validation != nil {
		l.Validation(txn, latched)
	}
}
