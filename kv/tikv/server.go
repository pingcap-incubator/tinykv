package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/rowcodec"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

var _ tikvpb.TikvServer = new(Server)

type Server struct {
	innerServer InnerServer
	wg          sync.WaitGroup
	refCount    int32
	stopped     int32
}

type InnerServer interface {
	Setup(pdClient pd.Client)
	Start(pdClient pd.Client) error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []inner_server.Modify) error
	Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error)
	Raft(stream tikvpb.Tikv_RaftServer) error
	Snapshot(stream tikvpb.Tikv_SnapshotServer) error
}

func NewServer(innerServer InnerServer) *Server {
	return &Server{
		innerServer: innerServer,
	}
}

const requestMaxSize = 6 * 1024 * 1024

func (svr *Server) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mocktikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (svr *Server) Stop() {
	atomic.StoreInt32(&svr.stopped, 1)
	for {
		if atomic.LoadInt32(&svr.refCount) == 0 {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, nil
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, nil
}

type kvScanProcessor struct {
	skipVal bool
	buf     []byte
	pairs   []*kvrpcpb.KvPair
}

func (p *kvScanProcessor) Process(key, value []byte) (err error) {
	if rowcodec.IsRowKey(key) {
		p.buf, err = rowcodec.RowToOldRow(value, p.buf)
		if err != nil {
			return err
		}
		value = p.buf
	}
	p.pairs = append(p.pairs, &kvrpcpb.KvPair{
		Key:   safeCopy(key),
		Value: safeCopy(value),
	})
	return nil
}

func (p *kvScanProcessor) SkipValue() bool {
	return p.skipVal
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	return nil, nil
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return nil, nil
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return nil, nil
}

func (svr *Server) KvCleanup(ctx context.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return nil, nil
}

func (svr *Server) KvBatchGet(ctx context.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return nil, nil
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, nil
}

func (svr *Server) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return nil, nil
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, nil
}

// RawKV commands.
func (svr *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := svr.innerServer.Reader(req.Context)
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			resp.RegionError = regErr
		} else {
			resp.Error = err.Error()
		}
		return resp, nil
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			resp.NotFound = true
		} else {
			resp.Error = err.Error()
		}
	} else {
		resp.Value = val
	}

	return resp, nil
}

func (svr *Server) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	err := svr.innerServer.Write(req.Context, []inner_server.Modify{{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		}}})
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			resp.RegionError = regErr
		} else {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

func (svr *Server) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	err := svr.innerServer.Write(req.Context, []inner_server.Modify{{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		}}})
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			resp.RegionError = regErr
		} else {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

func (svr *Server) RawScan(ctx context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := svr.innerServer.Reader(req.Context)
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			resp.RegionError = regErr
		} else {
			resp.Error = err.Error()
		}
		return resp, nil
	}

	pairs := make([]*kvrpcpb.KvPair, 0)

	it := reader.IterCF(req.Cf)
	for it.Seek(req.StartKey); it.Valid() && len(pairs) < int(req.Limit); it.Next() {
		key := it.Item().KeyCopy(nil)
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, nil
		}

		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	resp.Kvs = pairs

	return resp, nil
}

// Raft commands (tikv <-> tikv).
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}
func (svr *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return svr.innerServer.Snapshot(stream)
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
	case *ErrKeyAlreadyExists:
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: x.Key,
			},
		}
	case *ErrConflict:
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:          x.StartTS,
				ConflictTs:       x.ConflictTS,
				ConflictCommitTs: x.ConflictCommitTS,
				Key:              x.Key,
			},
		}
	case *ErrDeadlock:
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockKey:         x.LockKey,
				LockTs:          x.LockTS,
				DeadlockKeyHash: x.DeadlockKeyHash,
			},
		}
	case *ErrCommitExpire:
		return &kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				StartTs:           x.StartTs,
				AttemptedCommitTs: x.CommitTs,
				Key:               x.Key,
				MinCommitTs:       x.MinCommitTs,
			},
		}
	case *ErrTxnNotFound:
		return &kvrpcpb.KeyError{
			TxnNotFound: &kvrpcpb.TxnNotFound{
				StartTs:    x.StartTS,
				PrimaryKey: x.PrimaryKey,
			},
		}
	case *ErrCommitPessimisticLock:
		return &kvrpcpb.KeyError{
			Abort: x.Error(),
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
