package inner_server

import (
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
)

// InnerServer represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type InnerServer interface {
	Start(pdClient pd.Client) error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (DBReader, error)
	Raft(stream tinykvpb.TinyKv_RaftServer) error
	Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error
}

type DBReader interface {
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
