package raftstore

// import (
// 	"bytes"
// 	"fmt"
// 	"testing"

// 	"github.com/coocood/badger"
// 	"github.com/pingcap-incubator/tinykv/kv/tikv/mvcc"
// 	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
// 	rfpb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
// 	"github.com/stretchr/testify/assert"
// )

// func TestRaftWriteBatch_PrewriteAndCommit(t *testing.T) {
// 	engines := newTestEngines(t)
// 	defer cleanUpTestEngineData(engines)
// 	apply := new(applier)
// 	applyCtx := newApplyContext("test", nil, engines, nil, NewDefaultConfig())
// 	wb := &raftWriteBatch{
// 		startTS:  100,
// 		commitTS: 0,
// 	}
// 	// Testing PreWriter

// 	longValue := [128]byte{101}

// 	values := [][]byte{
// 		[]byte("short value"),
// 		longValue[:],
// 		[]byte(""),
// 	}

// 	for i := 0; i < 3; i++ {
// 		primary := []byte(fmt.Sprintf("t%08d_r%08d", i, i))
// 		expectLock := mvcc.MvccLock{
// 			MvccLockHdr: mvcc.MvccLockHdr{
// 				StartTS:    100,
// 				TTL:        10,
// 				Op:         uint8(kvrpcpb.Op_Put),
// 				PrimaryLen: uint16(len(primary)),
// 			},
// 			Primary: primary,
// 			Value:   values[i],
// 		}
// 		wb.Prewrite(primary, &expectLock, false)
// 		_, _, err := apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
// 			Header:   new(rfpb.RaftRequestHeader),
// 			Requests: wb.requests,
// 		})
// 		assert.Nil(t, err)
// 		err = applyCtx.wb.WriteToDB(engines.Kv)
// 		assert.Nil(t, err)
// 		applyCtx.wb.Reset()
// 		wb.requests = nil
// 		val := engines.Kv.LockStore.Get(primary, nil)
// 		assert.NotNil(t, val)
// 		lock := mvcc.DecodeLock(val)
// 		assert.Equal(t, expectLock, lock)
// 	}

// 	// Testing Commit
// 	wb = &raftWriteBatch{
// 		startTS:  100,
// 		commitTS: 200,
// 	}
// 	for i := 0; i < 3; i++ {
// 		primary := []byte(fmt.Sprintf("t%08d_r%08d", i, i))
// 		expectLock := &mvcc.MvccLock{
// 			MvccLockHdr: mvcc.MvccLockHdr{
// 				StartTS: 100,
// 				TTL:     10,
// 				Op:      uint8(mvcc.LockTypePut),
// 			},
// 			Value: values[i],
// 		}
// 		wb.Commit(primary, expectLock)
// 		apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
// 			Header:   new(rfpb.RaftRequestHeader),
// 			Requests: wb.requests,
// 		})
// 		err := applyCtx.wb.WriteToDB(engines.Kv)
// 		assert.Nil(t, err)
// 		applyCtx.wb.Reset()
// 		wb.requests = nil
// 		engines.Kv.DB.View(func(txn *badger.Txn) error {
// 			item, err := txn.Get(primary)
// 			assert.Nil(t, err)
// 			curVal, err := item.Value()
// 			assert.Nil(t, err)
// 			assert.NotNil(t, item)
// 			userMeta := mvcc.DBUserMeta(item.UserMeta())
// 			assert.Equal(t, userMeta.StartTS(), expectLock.StartTS)
// 			assert.Equal(t, userMeta.CommitTS(), wb.commitTS)
// 			assert.Equal(t, 0, bytes.Compare(curVal, expectLock.Value))
// 			return nil
// 		})
// 	}
// }

// func TestRaftWriteBatch_Rollback(t *testing.T) {
// 	engines := newTestEngines(t)
// 	defer cleanUpTestEngineData(engines)
// 	apply := new(applier)
// 	applyCtx := newApplyContext("test", nil, engines, nil, NewDefaultConfig())
// 	wb := &raftWriteBatch{
// 		startTS:  100,
// 		commitTS: 0,
// 	}

// 	longValue := [128]byte{102}

// 	for i := 0; i < 2; i++ {
// 		primary := []byte(fmt.Sprintf("t%08d_r%08d", i, i))
// 		expectLock := mvcc.MvccLock{
// 			MvccLockHdr: mvcc.MvccLockHdr{
// 				StartTS:    100,
// 				TTL:        10,
// 				Op:         uint8(kvrpcpb.Op_Put),
// 				PrimaryLen: uint16(len(primary)),
// 			},
// 			Primary: primary,
// 			Value:   longValue[:],
// 		}
// 		wb.Prewrite(primary, &expectLock, false)
// 		apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
// 			Header:   new(rfpb.RaftRequestHeader),
// 			Requests: wb.requests,
// 		})
// 		err := applyCtx.wb.WriteToDB(engines.Kv)
// 		assert.Nil(t, err)
// 		applyCtx.wb.Reset()
// 		wb.requests = nil
// 	}

// 	// Testing RollBack
// 	wb = &raftWriteBatch{
// 		startTS:  150,
// 		commitTS: 200,
// 	}
// 	primary := []byte(fmt.Sprintf("t%08d_r%08d", 0, 0))
// 	wb.Rollback(primary, false)
// 	apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
// 		Header:   new(rfpb.RaftRequestHeader),
// 		Requests: wb.requests,
// 	})
// 	err := applyCtx.wb.WriteToDB(engines.Kv)
// 	assert.Nil(t, err)
// 	applyCtx.wb.Reset()

// 	wb = &raftWriteBatch{
// 		startTS:  100,
// 		commitTS: 200,
// 	}
// 	primary = []byte(fmt.Sprintf("t%08d_r%08d", 1, 1))
// 	wb.Rollback(primary, true)
// 	apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
// 		Header:   new(rfpb.RaftRequestHeader),
// 		Requests: wb.requests,
// 	})
// 	err = applyCtx.wb.WriteToDB(engines.Kv)
// 	assert.Nil(t, err)
// 	applyCtx.wb.Reset()
// 	// The lock should be deleted.
// 	val := engines.Kv.LockStore.Get(primary, nil)
// 	assert.Nil(t, val)
// }
