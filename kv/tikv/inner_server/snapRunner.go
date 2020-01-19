package inner_server

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"github.com/pingcap/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type sendSnapTask struct {
	addr     string
	msg      *raft_serverpb.RaftMessage
	callback func(error)
}

type recvSnapTask struct {
	stream   tikvpb.Tikv_SnapshotServer
	callback func(error)
}

type snapRunner struct {
	config         *config.Config
	snapManager    *snap.SnapManager
	router         message.RaftRouter
	sendingCount   int64
	receivingCount int64
}

func newSnapRunner(snapManager *snap.SnapManager, config *config.Config, router message.RaftRouter) *snapRunner {
	return &snapRunner{
		config:      config,
		snapManager: snapManager,
		router:      router,
	}
}

func (r *snapRunner) Handle(t worker.Task) {
	switch t.Tp {
	case worker.TaskTypeSnapSend:
		r.send(t.Data.(sendSnapTask))
	case worker.TaskTypeSnapRecv:
		r.recv(t.Data.(recvSnapTask))
	}
}

func (r *snapRunner) send(t sendSnapTask) {
	if n := atomic.LoadInt64(&r.sendingCount); n > int64(r.config.ConcurrentSendSnapLimit) {
		log.Warnf("too many sending snapshot tasks, drop send snap [to: %v, snap: %v]", t.addr, t.msg)
		t.callback(errors.New("too many sending snapshot tasks"))
		return
	}

	atomic.AddInt64(&r.sendingCount, 1)
	defer atomic.AddInt64(&r.sendingCount, -1)
	t.callback(r.sendSnap(t.addr, t.msg))
}

const snapChunkLen = 1024 * 1024

func (r *snapRunner) sendSnap(addr string, msg *raft_serverpb.RaftMessage) error {
	start := time.Now()
	msgSnap := msg.GetMessage().GetSnapshot()
	snapKey, err := snap.SnapKeyFromSnap(msgSnap)
	if err != nil {
		return err
	}

	r.snapManager.Register(snapKey, snap.SnapEntrySending)
	defer r.snapManager.Deregister(snapKey, snap.SnapEntrySending)

	snap, err := r.snapManager.GetSnapshotForSending(snapKey)
	if err != nil {
		return err
	}
	if !snap.Exists() {
		return errors.Errorf("missing snap file: %v", snap.Path())
	}

	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(int32(r.config.GrpcInitialWindowSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    r.config.GrpcKeepAliveTime,
			Timeout: r.config.GrpcKeepAliveTimeout,
		}))
	if err != nil {
		return err
	}
	client := tikvpb.NewTikvClient(cc)
	stream, err := client.Snapshot(context.TODO())
	if err != nil {
		return err
	}
	err = stream.Send(&raft_serverpb.SnapshotChunk{Message: msg})
	if err != nil {
		return err
	}

	buf := make([]byte, snapChunkLen)
	for remain := snap.TotalSize(); remain > 0; remain -= uint64(len(buf)) {
		if remain < uint64(len(buf)) {
			buf = buf[:remain]
		}
		_, err := io.ReadFull(snap, buf)
		if err != nil {
			return errors.Errorf("failed to read snapshot chunk: %v", err)
		}
		err = stream.Send(&raft_serverpb.SnapshotChunk{Data: buf})
		if err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		return err
	}

	log.Infof("sent snapshot. regionID: %v, snapKey: %v, size: %v, duration: %s", snapKey.RegionID, snapKey, snap.TotalSize(), time.Since(start))
	return nil
}

func (r *snapRunner) recv(t recvSnapTask) {
	if n := atomic.LoadInt64(&r.receivingCount); n > int64(r.config.ConcurrentRecvSnapLimit) {
		log.Warnf("too many recving snapshot tasks, ignore")
		t.callback(errors.New("too many recving snapshot tasks"))
		return
	}
	atomic.AddInt64(&r.receivingCount, 1)
	defer atomic.AddInt64(&r.receivingCount, -1)
	msg, err := r.recvSnap(t.stream)
	if err == nil {
		r.router.SendRaftMessage(msg)
	}
	t.callback(err)
}

func (r *snapRunner) recvSnap(stream tikvpb.Tikv_SnapshotServer) (*raft_serverpb.RaftMessage, error) {
	head, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if head.GetMessage() == nil {
		return nil, errors.New("no raft message in the first chunk")
	}
	message := head.GetMessage().GetMessage()
	snapKey, err := snap.SnapKeyFromSnap(message.GetSnapshot())
	if err != nil {
		return nil, errors.Errorf("failed to create snap key: %v", err)
	}

	data := message.GetSnapshot().GetData()
	snapshot, err := r.snapManager.GetSnapshotForReceiving(snapKey, data)
	if err != nil {
		return nil, errors.Errorf("%v failed to create snapshot file: %v", snapKey, err)
	}
	if snapshot.Exists() {
		log.Infof("snapshot file already exists, skip receiving. snapKey: %v, file: %v", snapKey, snapshot.Path())
		stream.SendAndClose(&raft_serverpb.Done{})
		return head.GetMessage(), nil
	}
	r.snapManager.Register(snapKey, snap.SnapEntryReceiving)
	defer r.snapManager.Deregister(snapKey, snap.SnapEntryReceiving)

	for {
		chunk, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		data := chunk.GetData()
		if len(data) == 0 {
			return nil, errors.Errorf("%v receive chunk with empty data", snapKey)
		}
		_, err = bytes.NewReader(data).WriteTo(snapshot)
		if err != nil {
			return nil, errors.Errorf("%v failed to write snapshot file %v: %v", snapKey, snapshot.Path(), err)
		}
	}

	err = snapshot.Save()
	if err != nil {
		return nil, err
	}

	stream.SendAndClose(&raft_serverpb.Done{})
	return head.GetMessage(), nil
}
