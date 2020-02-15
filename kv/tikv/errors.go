package tikv

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key     []byte
	Primary []byte
	StartTS uint64
	TTL     uint64
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, primary: %q, startTS: %v", e.Key, e.Primary, e.StartTS)
}

// ErrRetryable suggests that client may restart the txn. e.g. write conflict.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

var (
	ErrLockNotFound    = ErrRetryable("lock not found")
	ErrAlreadyRollback = ErrRetryable("already rollback")
	ErrReplaced        = ErrRetryable("replaced by another transaction")
)

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed lock.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

type ErrKeyAlreadyExists struct {
	Key []byte
}

func (e ErrKeyAlreadyExists) Error() string {
	return "key already exists"
}

type ErrConflict struct {
	StartTS          uint64
	ConflictTS       uint64
	ConflictCommitTS uint64
	Key              []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}

// ErrTxnNotFound is returned if the required txn info not found on storage
type ErrTxnNotFound struct {
	StartTS    uint64
	PrimaryKey []byte
}

func (e *ErrTxnNotFound) Error() string {
	return "txn not found"
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if err == nil {
		return nil
	}
	causeErr := errors.Cause(err)
	switch x := causeErr.(type) {
	case *ErrLocked:
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:         x.Key,
				PrimaryLock: x.Primary,
				LockVersion: x.StartTS,
				LockTtl:     x.TTL,
			},
		}
	case ErrRetryable:
		return &kvrpcpb.KeyError{
			Retryable: x.Error(),
		}
	case *ErrConflict:
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    x.StartTS,
				ConflictTs: x.ConflictTS,
				Key:        x.Key,
			},
		}
	default:
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
}

func convertToPBError(err error) (*kvrpcpb.KeyError, *errorpb.Error) {
	if regErr := extractRegionError(err); regErr != nil {
		return nil, regErr
	}
	return convertToKeyError(err), nil
}

func convertToPBErrors(err error) ([]*kvrpcpb.KeyError, *errorpb.Error) {
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			return nil, regErr
		}
		return []*kvrpcpb.KeyError{convertToKeyError(err)}, nil
	}
	return nil, nil
}

func extractRegionError(err error) *errorpb.Error {
	if regionError, ok := err.(*inner_server.RegionError); ok {
		return regionError.RequestErr
	}
	return nil
}
