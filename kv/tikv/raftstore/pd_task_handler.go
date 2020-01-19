package raftstore

import (
	"context"

	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/shirou/gopsutil/disk"
)

type pdTaskHandler struct {
	storeID  uint64
	pdClient pd.Client
	router   message.RaftRouter
}

func newPDTaskHandler(storeID uint64, pdClient pd.Client, router message.RaftRouter) *pdTaskHandler {
	return &pdTaskHandler{
		storeID:  storeID,
		pdClient: pdClient,
		router:   router,
	}
}

func (r *pdTaskHandler) Handle(t worker.Task) {
	switch t.Tp {
	case worker.TaskTypePDAskBatchSplit:
		r.onAskBatchSplit(t.Data.(*pdAskBatchSplitTask))
	case worker.TaskTypePDHeartbeat:
		r.onHeartbeat(t.Data.(*pdRegionHeartbeatTask))
	case worker.TaskTypePDStoreHeartbeat:
		r.onStoreHeartbeat(t.Data.(*pdStoreHeartbeatTask))
	case worker.TaskTypePDReportBatchSplit:
		r.onReportBatchSplit(t.Data.(*pdReportBatchSplitTask))
	case worker.TaskTypePDDestroyPeer:
		r.onDestroyPeer(t.Data.(*pdDestroyPeerTask))
	default:
		log.Error("unsupported worker.Task type:", t.Tp)
	}
}

func (r *pdTaskHandler) start() {
	r.pdClient.SetRegionHeartbeatResponseHandler(r.onRegionHeartbeatResponse)
}

func (r *pdTaskHandler) onRegionHeartbeatResponse(resp *pdpb.RegionHeartbeatResponse) {
	if changePeer := resp.GetChangePeer(); changePeer != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerRequest{
				ChangeType: changePeer.ChangeType,
				Peer:       changePeer.Peer,
			},
		}, message.NewCallback())
	} else if transferLeader := resp.GetTransferLeader(); transferLeader != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderRequest{
				Peer: transferLeader.Peer,
			},
		}, message.NewCallback())
	}
}

func (r *pdTaskHandler) onAskBatchSplit(t *pdAskBatchSplitTask) {
	resp, err := r.pdClient.AskBatchSplit(context.TODO(), t.region, len(t.splitKeys))
	if err != nil {
		log.Error(err)
		return
	}
	srs := make([]*raft_cmdpb.SplitRequest, len(resp.Ids))
	for i, splitID := range resp.Ids {
		srs[i] = &raft_cmdpb.SplitRequest{
			SplitKey:    t.splitKeys[i],
			NewRegionId: splitID.NewRegionId,
			NewPeerIds:  splitID.NewPeerIds,
		}
	}
	aq := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_BatchSplit,
		Splits: &raft_cmdpb.BatchSplitRequest{
			Requests: srs,
		},
	}
	r.sendAdminRequest(t.region.GetId(), t.region.GetRegionEpoch(), t.peer, aq, t.callback)
}

func (r *pdTaskHandler) onHeartbeat(t *pdRegionHeartbeatTask) {
	var size, keys int64
	if t.approximateSize != nil {
		size = int64(*t.approximateSize)
	}

	req := &pdpb.RegionHeartbeatRequest{
		Region:          t.region,
		Leader:          t.peer,
		DownPeers:       t.downPeers,
		PendingPeers:    t.pendingPeers,
		ApproximateSize: uint64(size),
		ApproximateKeys: uint64(keys),
	}
	r.pdClient.ReportRegion(req)
}

func (r *pdTaskHandler) onStoreHeartbeat(t *pdStoreHeartbeatTask) {
	diskStat, err := disk.Usage(t.path)
	if err != nil {
		log.Error(err)
		return
	}

	capacity := t.capacity
	if capacity == 0 || diskStat.Total < capacity {
		capacity = diskStat.Total
	}
	lsmSize, vlogSize := t.engine.Size()
	usedSize := t.stats.UsedSize + uint64(lsmSize) + uint64(vlogSize) // t.stats.UsedSize contains size of snapshot files.
	available := uint64(0)
	if capacity > usedSize {
		available = capacity - usedSize
	}

	t.stats.Capacity = capacity
	t.stats.UsedSize = usedSize
	t.stats.Available = available

	r.pdClient.StoreHeartbeat(context.TODO(), t.stats)
}

func (r *pdTaskHandler) onReportBatchSplit(t *pdReportBatchSplitTask) {
	r.pdClient.ReportBatchSplit(context.TODO(), t.regions)
}

func (r *pdTaskHandler) onDestroyPeer(t *pdDestroyPeerTask) {
	// TODO: delete it
}

func (r *pdTaskHandler) sendAdminRequest(regionID uint64, epoch *metapb.RegionEpoch, peer *metapb.Peer, req *raft_cmdpb.AdminRequest, callback *message.Callback) {
	cmd := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId:    regionID,
			Peer:        peer,
			RegionEpoch: epoch,
		},
		AdminRequest: req,
	}
	r.router.SendRaftCommand(cmd, callback)
}

func (r *pdTaskHandler) sendDestroyPeer(local *metapb.Region, peer *metapb.Peer, pdRegion *metapb.Region) {
	r.router.Send(local.GetId(), message.Msg{
		Type:     message.MsgTypeRaftMessage,
		RegionID: local.GetId(),
		Data: &raft_serverpb.RaftMessage{
			RegionId:    local.GetId(),
			FromPeer:    peer,
			ToPeer:      peer,
			RegionEpoch: pdRegion.GetRegionEpoch(),
			IsTombstone: true,
		},
	})
}
