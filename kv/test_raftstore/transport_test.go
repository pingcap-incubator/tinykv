package test_raftstore

import (
	"bytes"
	"log"
	"testing"
)

func testPartitionWrite(cluster *Cluster) {
	err := cluster.Start()
	if err != nil {
		panic(err)
	}

	key := []byte("k1")
	value := []byte("v1")
	cluster.MustPut(key, value)
	MustGetEqual(cluster.engines[1], key, value)

	regionID := cluster.GetRegion(key).GetId()

	// transfer leader to (1, 1)
	peer := NewPeer(1, 1)
	cluster.MustTransferLeader(regionID, &peer)

	// leader in majority, partition doesn't affect write/read
	cluster.Partition([]uint64{1, 2, 3}, []uint64{4, 5})
	cluster.MustPut(key, value)
	val := cluster.Get(key)
	if bytes.Compare(val, value) != 0 {
		log.Panic(val, value)
	}
	cluster.MustTransferLeader(regionID, &peer)
	cluster.ClearFilters()

	// leader in minority, new leader should be elected
	cluster.Partition([]uint64{1, 2}, []uint64{3, 4, 5})
	val = cluster.MustGet(key)
	if bytes.Compare(val, value) != 0 {
		log.Panic(val, value)
	}
	leaderID := cluster.LeaderOfRegion(regionID).Id
	if leaderID == 1 || leaderID == 2 {
		log.Panic(leaderID, "leaderID == 1 || leaderID == 2")
	}
	cluster.MustPut(key, []byte("changed"))
	cluster.ClearFilters()

	cluster.MustPut([]byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[1], []byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[1], key, []byte("changed"))

	cluster.Shutdown()
}

func TestNodePartitionWrite(t *testing.T) {
	pdClient := NewMockPDClient(0)
	simulator := NewNodeSimulator(pdClient)
	cluster := NewCluster(5, pdClient, &simulator)
	testPartitionWrite(cluster)
}
