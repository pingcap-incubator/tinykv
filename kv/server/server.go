package server

import (
	"context"
	"reflect"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/inner_server/raft_server"
	"github.com/pingcap-incubator/tinykv/kv/transaction/commands"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	innerServer inner_server.InnerServer
	Latches     *latches.Latches
}

func NewServer(innerServer inner_server.InnerServer) *Server {
	return &Server{
		innerServer: innerServer,
		Latches:     latches.NewLatches(),
	}
}

// Run runs a transactional command.
func (server *Server) Run(cmd commands.Command) (interface{}, error) {
	return commands.RunCommand(cmd, server.innerServer, server.Latches)
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// TODO: delete the bodies of the below functions.

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your code here 4A
	cmd := commands.NewGet(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.GetResponse), err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your code here 4B
	cmd := commands.NewScan(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.ScanResponse), err
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your code here 4A
	cmd := commands.NewPrewrite(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.PrewriteResponse), err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your code here 4A
	cmd := commands.NewCommit(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.CommitResponse), err
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your code here 4B
	cmd := commands.NewCheckTxnStatus(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.CheckTxnStatusResponse), err
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your code here 4B
	cmd := commands.NewRollback(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.BatchRollbackResponse), err
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your code here 4B
	cmd := commands.NewResolveLock(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.ResolveLockResponse), err
}

// Raw API. These commands are handled inline rather than by using Run and am implementation of the Commands interface.
// This is because these commands are fairly straightforward and do not share a lot of code with the transactional
// commands.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your code here 1A
	response := new(kvrpcpb.RawGetResponse)
	reader, err := server.innerServer.Reader(req.Context)
	if !rawRegionError(err, response) {
		val, err := reader.GetCF(req.Cf, req.Key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				response.NotFound = true
			} else {
				rawRegionError(err, response)
			}
		} else {
			response.Value = val
		}
	}

	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your code here 1A
	response := new(kvrpcpb.RawPutResponse)
	err := server.innerServer.Write(req.Context, []inner_server.Modify{{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		}}})
	rawRegionError(err, response)
	return response, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your code here 1A
	response := new(kvrpcpb.RawDeleteResponse)
	err := server.innerServer.Write(req.Context, []inner_server.Modify{{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		}}})
	rawRegionError(err, response)
	return response, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your code here 1A
	response := new(kvrpcpb.RawScanResponse)

	reader, err := server.innerServer.Reader(req.Context)
	if !rawRegionError(err, response) {
		// To scan, we need to get an iterator for the underlying storage.
		it := reader.IterCF(req.Cf)
		defer it.Close()
		// Initialize the iterator. Termination condition is that the iterator is still valid (i.e.
		// we have not reached the end of the DB) and we haven't exceeded the client-specified limit.
		for it.Seek(req.StartKey); it.Valid() && len(response.Kvs) < int(req.Limit); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				rawRegionError(err, response)
				break
			} else {
				response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
					Key:   key,
					Value: value,
				})
			}
		}
	}

	return response, nil
}

// Raft commands (tinykv <-> tinykv); these are trivially forwarded to innerServer.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.innerServer.(*raft_server.RaftInnerServer).Raft(stream)
}

func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.innerServer.(*raft_server.RaftInnerServer).Snapshot(stream)
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	return &coprocessor.Response{}, nil
}

// rawRegionError assigns region errors to a RegionError field, and other errors to the Error field,
// of resp. This is only a valid way to handle errors for the raw commands. Returns true if err is
// non-nil, false otherwise.
func rawRegionError(err error, resp interface{}) bool {
	if err == nil {
		return false
	}
	respValue := reflect.ValueOf(resp)
	if regionErr, ok := err.(*raft_server.RegionError); ok {
		respValue.FieldByName("RegionError").Set(reflect.ValueOf(regionErr.RequestErr))
	} else {
		respValue.FieldByName("Error").Set(reflect.ValueOf(err.Error()))
	}
	return true
}
