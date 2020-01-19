package raftstore

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// router routes a message to a peer.
type router struct {
	peers         sync.Map
	workerSenders []chan message.Msg
	storeSender   chan<- message.Msg
	storeFsm      *storeFsm
}

func newRouter(workerSize int, storeSender chan<- message.Msg, storeFsm *storeFsm) *router {
	pm := &router{
		workerSenders: make([]chan message.Msg, workerSize),
		storeSender:   storeSender,
		storeFsm:      storeFsm,
	}
	for i := 0; i < workerSize; i++ {
		pm.workerSenders[i] = make(chan message.Msg, 4096)
	}
	return pm
}

func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

func (pr *router) register(peer *peerFsm) {
	id := peer.peer.regionId
	idx := int(id) % len(pr.workerSenders)
	apply := newApplierFromPeer(peer)
	newPeer := &peerState{
		msgCh:  pr.workerSenders[idx],
		closed: atomic.NewBool(false),
		peer:   peer,
		apply:  apply,
	}
	pr.peers.Store(id, newPeer)
}

func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		ps.close()
		pr.peers.Delete(regionID)
	}
}

func (pr *router) send(regionID uint64, msg message.Msg) error {
	msg.RegionID = regionID
	p := pr.get(regionID)
	if p == nil {
		return errPeerNotFound
	}
	return p.send(msg)
}

func (pr *router) sendRaftCommand(cmd *message.MsgRaftCmd) error {
	regionID := cmd.Request.Header.RegionId
	return pr.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}

func (pr *router) sendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	if pr.send(regionID, message.NewPeerMsg(message.MsgTypeRaftMessage, regionID, msg)) != nil {
		pr.sendStore(message.NewPeerMsg(message.MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil
}

func (pr *router) sendStore(msg message.Msg) {
	pr.storeSender <- msg
}

var errPeerNotFound = errors.New("peer not found")
