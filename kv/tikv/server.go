package tikv

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

var _ tikvpb.TikvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	innerServer InnerServer
	scheduler   Scheduler
	refCount    int32
	stopped     int32
}

// InnerServer represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type InnerServer interface {
	Start(pdClient pd.Client) error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []inner_server.Modify) error
	Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error)
	Raft(stream tikvpb.Tikv_RaftServer) error
	Snapshot(stream tikvpb.Tikv_SnapshotServer) error
}

// Scheduler takes Commands and runs them asynchronously. It is up to implementations to decide the scheduling policy.
type Scheduler interface {
	Run(Command) <-chan RespResult
	Stop()
}

// RespResult is a 'generic' result type for responses. It is used to return a Response/error pair over channels where
// we can't use Go's multiple return values.
type RespResult struct {
	Response interface{}
	Err      error
}

func RespOk(resp interface{}) RespResult {
	return RespResult{
		Response: resp,
		Err:      nil,
	}
}

func RespErr(err error) RespResult {
	return RespResult{
		Response: nil,
		Err:      err,
	}
}

// Command is an abstraction which covers the process from receiving a request from gRPC to returning a response.
// That process is driven by a Scheduler.
type Command interface {
	BuildTxn(txn *kvstore.Txn) error
	Context() *kvrpcpb.Context
	Response() (interface{}, error)
	RegionError(*errorpb.Error) interface{}
}

func NewServer(innerServer InnerServer, scheduler Scheduler) *Server {
	return &Server{
		innerServer: innerServer,
		scheduler:   scheduler,
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

func (svr *Server) Stop() error {
	atomic.StoreInt32(&svr.stopped, 1)
	for {
		if atomic.LoadInt32(&svr.refCount) == 0 {
			svr.scheduler.Stop()
			return svr.innerServer.Stop()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// The below functions are Server's gRPC API (implements TikvServer).

// Transactional API.
func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, nil
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, nil
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

// Raw API.
func (svr *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	cmd := commands.NewRawGet(req)
	resp := <-svr.scheduler.Run(&cmd)
	if resp.Err != nil {
		return nil, resp.Err
	}

	return resp.Response.(*kvrpcpb.RawGetResponse), nil
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
		if regErr := ExtractRegionError(err); regErr != nil {
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
		if regErr := ExtractRegionError(err); regErr != nil {
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
		if regErr := ExtractRegionError(err); regErr != nil {
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

// Raft commands (tikv <-> tikv); these are trivially forwarded to innerServer.
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}

func (svr *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return svr.innerServer.Snapshot(stream)
}
