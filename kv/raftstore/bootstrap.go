package raftstore

import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
)

func isRangeEmpty(engine *badger.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	err := engine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(startKey)
		if it.Valid() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) < 0 {
				hasData = true
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	return !hasData, err
}

func BootstrapStore(engines *engine_util.Engines, clusterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isRangeEmpty(engines.Kv, meta.MinKey, meta.MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and has already had data.")
	}
	empty, err = isRangeEmpty(engines.Raft, meta.MinKey, meta.MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clusterID
	ident.StoreId = storeID
	err = engine_util.PutMeta(engines.Kv, meta.StoreIdentKey, ident)
	if err != nil {
		return err
	}
	return nil
}

func PrepareBootstrap(engines *engine_util.Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte{},
		EndKey:   []byte{},
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: []*metapb.Peer{
			{
				Id:      peerID,
				StoreId: storeID,
			},
		},
	}
	err := PrepareBootstrapCluster(engines, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func PrepareBootstrapCluster(engines *engine_util.Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	kvWB := new(engine_util.WriteBatch)
	kvWB.SetMeta(meta.PrepareBootstrapKey, state)
	kvWB.SetMeta(meta.RegionStateKey(region.Id), state)
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	raftWB := new(engine_util.WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return nil
}

func writeInitialApplyState(kvWB *engine_util.WriteBatch, regionID uint64) {
	applyState := &rspb.RaftApplyState{
		AppliedIndex: meta.RaftInitLogIndex,
		TruncatedState: &rspb.RaftTruncatedState{
			Index: meta.RaftInitLogIndex,
			Term:  meta.RaftInitLogTerm,
		},
	}
	kvWB.SetMeta(meta.ApplyStateKey(regionID), applyState)
}

func writeInitialRaftState(raftWB *engine_util.WriteBatch, regionID uint64) {
	raftState := &rspb.RaftLocalState{
		HardState: &eraftpb.HardState{
			Term:   meta.RaftInitLogTerm,
			Commit: meta.RaftInitLogIndex,
		},
		LastIndex: meta.RaftInitLogIndex,
	}
	raftWB.SetMeta(meta.RaftStateKey(regionID), raftState)
}

func ClearPrepareBootstrap(engines *engine_util.Engines, regionID uint64) error {
	err := engines.Raft.Update(func(txn *badger.Txn) error {
		return txn.Delete(meta.RaftStateKey(regionID))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	wb := new(engine_util.WriteBatch)
	wb.DeleteMeta(meta.PrepareBootstrapKey)
	// should clear raft initial state too.
	wb.DeleteMeta(meta.RegionStateKey(regionID))
	wb.DeleteMeta(meta.ApplyStateKey(regionID))
	err = engines.WriteKV(wb)
	if err != nil {
		return err
	}
	return nil
}

func ClearPrepareBootstrapState(engines *engine_util.Engines) error {
	err := engines.Kv.Update(func(txn *badger.Txn) error {
		return txn.Delete(meta.PrepareBootstrapKey)
	})
	return errors.WithStack(err)
}
