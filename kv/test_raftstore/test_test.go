package test_raftstore

import (
	"bytes"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
)

// a client runs the function f and then signals it is done
func runClient(t *testing.T, me int, ca chan bool, fn func(me int, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	fn(me, t)
	ok = true
}

// spawn ncli clients and wait until they are all done
func SpawnClientsAndWait(t *testing.T, ch chan bool, ncli int, fn func(me int, t *testing.T)) {
	defer func() { ch <- true }()
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go runClient(t, cli, ca[cli], fn)
	}
	// log.Printf("SpawnClientsAndWait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Infof("SpawnClientsAndWait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}

}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
func partitioner(t *testing.T, cluster *Cluster, ch chan bool, done *int32, unreliable bool, partitions bool, electionTimeout time.Duration) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		if partitions {
			a := make([]int, cluster.count)
			for i := 0; i < cluster.count; i++ {
				a[i] = (rand.Int() % 2)
			}
			pa := make([][]uint64, 2)
			for i := 0; i < 2; i++ {
				pa[i] = make([]uint64, 0)
				for j := 1; j <= cluster.count; j++ {
					if a[j-1] == i {
						pa[i] = append(pa[i], uint64(j))
					}
				}
			}
			cluster.ClearFilters()
			log.Infof("partition: %v, %v", pa[0], pa[1])
			cluster.AddFilter(&PartitionFilter{
				s1: pa[0],
				s2: pa[1],
			})
		}

		if unreliable {
			cluster.AddFilter(&DropFilter{})
		}
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

func confchanger(t *testing.T, cluster *Cluster, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	count := uint64(cluster.count)
	for atomic.LoadInt32(done) == 0 {
		region := cluster.GetRandomRegion()
		store := rand.Uint64()%count + 1
		if p := FindPeer(region, store); p != nil {
			if len(region.GetPeers()) > 1 {
				cluster.MustRemovePeer(region.GetId(), p)
			}
		} else {
			cluster.MustAddPeer(region.GetId(), cluster.AllocPeer(store))
		}
		time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
	}
}

// Basic test is as follows: one or more clients submitting Put/Scan
// operations to set of servers for some period of time.  After the period is
// over, test checks that all sequential values are present and in order for a
// particular key and perform Delete to clean up.
// - If unreliable is set, RPCs may fail.
// - If crash is set, the servers restart after the period is over.
// - If partitions is set, the test repartitions the network concurrently between the servers.
// - If maxraftlog is a positive number, the count of the persistent log for Raft shouldn't exceed 2*maxraftlog.
// - If confchangee is set, the cluster will schedule random conf change concurrently.
// - If split is set, split region when size exceed 1024 bytes.
func GenericTest(t *testing.T, part string, nclients int, unreliable bool, crash bool, partitions bool, maxraftlog int, confchange bool, split bool) {
	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftlog != -1 {
		title = title + "snapshots, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")" // 3A or 3B

	nservers := 5
	cfg := config.NewTestConfig()
	if maxraftlog != -1 {
		cfg.RaftLogGcCountLimit = uint64(maxraftlog)
	}
	if split {
		cfg.RegionMaxSize = 300
		cfg.RegionSplitSize = 200
	}
	cluster := NewTestCluster(nservers, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	electionTimeout := cfg.RaftBaseTickInterval * time.Duration(cfg.RaftElectionTimeoutTicks)
	// Wait for leader election
	time.Sleep(2 * electionTimeout)

	done_partitioner := int32(0)
	done_confchanger := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	ch_confchange := make(chan bool)
	ch_clients := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int, 1)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go SpawnClientsAndWait(t, ch_clients, nclients, func(cli int, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 {
					key := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", j)
					value := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Infof("%d: client new put %v,%v\n", cli, key, value)
					cluster.MustPut([]byte(key), []byte(value))
					last = NextValue(last, value)
					j++
				} else {
					start := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", 0)
					end := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", j)
					// log.Infof("%d: client new scan %v-%v\n", cli, start, end)
					values := cluster.Scan([]byte(start), []byte(end))
					v := string(bytes.Join(values, []byte("")))
					if v != last {
						log.Fatalf("get wrong value, client %v\nwant:%v\ngot: %v\n", cli, last, v)
					}
				}
			}
		})

		// enable cases:
		//               partitions    no partitions
		//    unreliable       1                1
		// no unreliable       1                1
		if unreliable || partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(300 * time.Millisecond)
			go partitioner(t, cluster, ch_partitioner, &done_partitioner, unreliable, partitions, electionTimeout)
		}
		if confchange {
			// Allow the clients to perfrom some operations without interruption
			time.Sleep(100 * time.Millisecond)
			go confchanger(t, cluster, ch_confchange, &done_confchanger)
		}
		time.Sleep(5 * time.Second)
		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit
		atomic.StoreInt32(&done_confchanger, 1) // tell confchanger to quit

		if unreliable || partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cluster.ClearFilters()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		// log.Printf("wait for clients\n")
		<-ch_clients

		if crash {
			log.Warnf("shutdown servers\n")
			for i := 1; i <= nservers; i++ {
				cluster.StopServer(uint64(i))
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			log.Warnf("restart servers\n")
			// crash and re-start all
			for i := 1; i <= nservers; i++ {
				cluster.StartServer(uint64(i))
			}
		}

		for cli := 0; cli < nclients; cli++ {
			// log.Printf("read from clients %d\n", cli)
			j := <-clnts[cli]

			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			start := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", 0)
			end := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", j)
			values := cluster.Scan([]byte(start), []byte(end))
			v := string(bytes.Join(values, []byte("")))
			checkClntAppends(t, cli, v, j)

			for k := 0; k < j; k++ {
				key := strconv.Itoa(cli) + " " + fmt.Sprintf("%08d", k)
				cluster.MustDelete([]byte(key))
			}
		}

		if maxraftlog > 0 {
			time.Sleep(1 * time.Second)

			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			key := []byte("")
			for {
				region := cluster.GetRegion(key)
				if region == nil {
					panic("region is not found")
				}
				for _, engine := range cluster.engines {
					state, err := meta.GetApplyState(engine.Kv, region.GetId())
					if err == badger.ErrKeyNotFound {
						continue
					}
					if err != nil {
						panic(err)
					}
					truncatedIdx := state.TruncatedState.Index
					appliedIdx := state.AppliedIndex
					if appliedIdx-truncatedIdx > 2*uint64(maxraftlog) {
						t.Fatalf("logs were not trimmed (%v - %v > 2*%v)", appliedIdx, truncatedIdx, maxraftlog)
					}
				}

				key = region.EndKey
				if len(key) == 0 {
					break
				}
			}
		}

		if split {
			r := cluster.GetRegion([]byte(""))
			if len(r.GetEndKey()) == 0 {
				t.Fatalf("region is not split")
			}
		}
	}
}

func TestBasic2B(t *testing.T) {
	// Test: one client (2B) ...
	GenericTest(t, "2B", 1, false, false, false, -1, false, false)
}

func TestConcurrent2B(t *testing.T) {
	// Test: many clients (2B) ...
	GenericTest(t, "2B", 5, false, false, false, -1, false, false)
}

func TestUnreliable2B(t *testing.T) {
	// Test: unreliable net, many clients (2B) ...
	GenericTest(t, "2B", 5, true, false, false, -1, false, false)
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition2B(t *testing.T) {
	cfg := config.NewTestConfig()
	cluster := NewTestCluster(5, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	region := cluster.GetRegion([]byte(""))
	leader := cluster.LeaderOfRegion(region.GetId())
	s1 := []uint64{leader.GetStoreId()}
	s2 := []uint64{}
	for _, p := range region.GetPeers() {
		if p.GetId() == leader.GetId() {
			continue
		}
		if len(s1) < 3 {
			s1 = append(s1, p.GetStoreId())
		} else {
			s2 = append(s2, p.GetStoreId())
		}
	}

	// leader in majority, partition doesn't affect write/read
	cluster.AddFilter(&PartitionFilter{
		s1: s1,
		s2: s2,
	})
	cluster.MustPut([]byte("k1"), []byte("v1"))
	cluster.MustGet([]byte("k1"), []byte("v1"))
	MustGetNone(cluster.engines[s2[0]], []byte("k1"))
	MustGetNone(cluster.engines[s2[1]], []byte("k1"))
	cluster.ClearFilters()

	// old leader in minority, new leader should be elected
	s2 = append(s2, s1[2])
	s1 = s1[:2]
	cluster.AddFilter(&PartitionFilter{
		s1: s1,
		s2: s2,
	})
	cluster.MustGet([]byte("k1"), []byte("v1"))
	cluster.MustPut([]byte("k1"), []byte("changed"))
	MustGetEqual(cluster.engines[s1[0]], []byte("k1"), []byte("v1"))
	MustGetEqual(cluster.engines[s1[1]], []byte("k1"), []byte("v1"))
	cluster.ClearFilters()

	// when partition heals, old leader should sync data
	cluster.MustPut([]byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[s1[0]], []byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[s1[0]], []byte("k1"), []byte("changed"))
}

func TestManyPartitionsOneClient2B(t *testing.T) {
	// Test: partitions, one client (2B) ...
	GenericTest(t, "2B", 1, false, false, true, -1, false, false)
}

func TestManyPartitionsManyClients2B(t *testing.T) {
	// Test: partitions, many clients (2B) ...
	GenericTest(t, "2B", 5, false, false, true, -1, false, false)
}

func TestPersistOneClient2B(t *testing.T) {
	// Test: restarts, one client (2B) ...
	GenericTest(t, "2B", 1, false, true, false, -1, false, false)
}

func TestPersistConcurrent2B(t *testing.T) {
	// Test: restarts, many clients (2B) ...
	GenericTest(t, "2B", 5, false, true, false, -1, false, false)
}

func TestPersistConcurrentUnreliable2B(t *testing.T) {
	// Test: unreliable net, restarts, many clients (2B) ...
	GenericTest(t, "2B", 5, true, true, false, -1, false, false)
}

func TestPersistPartition2B(t *testing.T) {
	// Test: restarts, partitions, many clients (2B) ...
	GenericTest(t, "2B", 5, false, true, true, -1, false, false)
}

func TestPersistPartitionUnreliable2B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (3A) ...
	GenericTest(t, "2B", 5, true, true, true, -1, false, false)
}

func TestOneSnapshot2C(t *testing.T) {
	cfg := config.NewTestConfig()
	cfg.RaftLogGcCountLimit = 10
	cluster := NewTestCluster(3, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	cf := engine_util.CfLock
	cluster.MustPutCF(cf, []byte("k1"), []byte("v1"))
	cluster.MustPutCF(cf, []byte("k2"), []byte("v2"))

	MustGetCfEqual(cluster.engines[1], cf, []byte("k1"), []byte("v1"))
	MustGetCfEqual(cluster.engines[1], cf, []byte("k2"), []byte("v2"))

	for _, engine := range cluster.engines {
		state, err := meta.GetApplyState(engine.Kv, 1)
		if err != nil {
			t.Fatal(err)
		}
		if state.TruncatedState.Index != meta.RaftInitLogIndex ||
			state.TruncatedState.Term != meta.RaftInitLogTerm {
			t.Fatalf("unexpected truncated state %v", state.TruncatedState)
		}
	}

	cluster.AddFilter(
		&PartitionFilter{
			s1: []uint64{1},
			s2: []uint64{2, 3},
		},
	)

	// write some data to trigger snapshot
	for i := 100; i < 115; i++ {
		cluster.MustPutCF(cf, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	cluster.MustDeleteCF(cf, []byte("k2"))
	time.Sleep(500 * time.Millisecond)
	MustGetCfNone(cluster.engines[1], cf, []byte("k100"))
	cluster.ClearFilters()

	// Now snapshot must applied on
	MustGetCfEqual(cluster.engines[1], cf, []byte("k1"), []byte("v1"))
	MustGetCfEqual(cluster.engines[1], cf, []byte("k100"), []byte("v100"))
	MustGetCfNone(cluster.engines[1], cf, []byte("k2"))

	cluster.StopServer(1)
	cluster.StartServer(1)

	MustGetCfEqual(cluster.engines[1], cf, []byte("k1"), []byte("v1"))
	for _, engine := range cluster.engines {
		state, err := meta.GetApplyState(engine.Kv, 1)
		if err != nil {
			t.Fatal(err)
		}
		truncatedIdx := state.TruncatedState.Index
		appliedIdx := state.AppliedIndex
		if appliedIdx-truncatedIdx > 2*uint64(cfg.RaftLogGcCountLimit) {
			t.Fatalf("logs were not trimmed (%v - %v > 2*%v)", appliedIdx, truncatedIdx, cfg.RaftLogGcCountLimit)
		}
	}
}

func TestSnapshotRecover2C(t *testing.T) {
	// Test: restarts, snapshots, one client (2C) ...
	GenericTest(t, "2C", 1, false, true, false, 100, false, false)
}

func TestSnapshotRecoverManyClients2C(t *testing.T) {
	// Test: restarts, snapshots, many clients (2C) ...
	GenericTest(t, "2C", 20, false, true, false, 100, false, false)
}

func TestSnapshotUnreliable2C(t *testing.T) {
	// Test: unreliable net, snapshots, many clients (2C) ...
	GenericTest(t, "2C", 5, true, false, false, 100, false, false)
}

func TestSnapshotUnreliableRecover2C(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, many clients (2C) ...
	GenericTest(t, "2C", 5, true, true, false, 100, false, false)
}

func TestSnapshotUnreliableRecoverConcurrentPartition2C(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, many clients (2C) ...
	GenericTest(t, "2C", 5, true, true, true, 100, false, false)
}

func TestTransferLeader3B(t *testing.T) {
	cfg := config.NewTestConfig()
	cluster := NewTestCluster(5, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	regionID := cluster.GetRegion([]byte("")).GetId()
	cluster.MustTransferLeader(regionID, NewPeer(1, 1))
	cluster.MustTransferLeader(regionID, NewPeer(2, 2))
	cluster.MustTransferLeader(regionID, NewPeer(3, 3))
	cluster.MustTransferLeader(regionID, NewPeer(4, 4))
	cluster.MustTransferLeader(regionID, NewPeer(5, 5))
}

func TestBasicConfChange3B(t *testing.T) {
	cfg := config.NewTestConfig()
	cluster := NewTestCluster(5, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	cluster.MustTransferLeader(1, NewPeer(1, 1))
	cluster.MustRemovePeer(1, NewPeer(2, 2))
	cluster.MustRemovePeer(1, NewPeer(3, 3))
	cluster.MustRemovePeer(1, NewPeer(4, 4))
	cluster.MustRemovePeer(1, NewPeer(5, 5))

	// now region 1 only has peer: (1, 1)
	cluster.MustPut([]byte("k1"), []byte("v1"))
	MustGetNone(cluster.engines[2], []byte("k1"))

	// add peer (2, 2) to region 1
	cluster.MustAddPeer(1, NewPeer(2, 2))
	cluster.MustPut([]byte("k2"), []byte("v2"))
	cluster.MustGet([]byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[2], []byte("k1"), []byte("v1"))
	MustGetEqual(cluster.engines[2], []byte("k2"), []byte("v2"))

	epoch := cluster.GetRegion([]byte("k1")).GetRegionEpoch()
	assert.True(t, epoch.GetConfVer() > 1)

	// peer 5 must not exist
	MustGetNone(cluster.engines[5], []byte("k1"))

	// add peer (3, 3) to region 1
	cluster.MustAddPeer(1, NewPeer(3, 3))
	cluster.MustRemovePeer(1, NewPeer(2, 2))

	cluster.MustPut([]byte("k3"), []byte("v3"))
	cluster.MustGet([]byte("k3"), []byte("v3"))
	MustGetEqual(cluster.engines[3], []byte("k1"), []byte("v1"))
	MustGetEqual(cluster.engines[3], []byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[3], []byte("k3"), []byte("v3"))

	// peer 2 has nothing
	MustGetNone(cluster.engines[2], []byte("k1"))
	MustGetNone(cluster.engines[2], []byte("k2"))

	cluster.MustAddPeer(1, NewPeer(2, 2))
	MustGetEqual(cluster.engines[2], []byte("k1"), []byte("v1"))
	MustGetEqual(cluster.engines[2], []byte("k2"), []byte("v2"))
	MustGetEqual(cluster.engines[2], []byte("k3"), []byte("v3"))

	// remove peer (2, 2) from region 1
	cluster.MustRemovePeer(1, NewPeer(2, 2))
	// add peer (2, 4) to region 1
	cluster.MustAddPeer(1, NewPeer(2, 4))
	// remove peer (3, 3) from region 1
	cluster.MustRemovePeer(1, NewPeer(3, 3))

	cluster.MustPut([]byte("k4"), []byte("v4"))
	MustGetEqual(cluster.engines[2], []byte("k1"), []byte("v1"))
	MustGetEqual(cluster.engines[2], []byte("k4"), []byte("v4"))
	MustGetNone(cluster.engines[3], []byte("k1"))
	MustGetNone(cluster.engines[3], []byte("k4"))
}

func TestConfChangeRecover3B(t *testing.T) {
	// Test: restarts, snapshots, conf change, one client (3B) ...
	GenericTest(t, "3B", 1, false, true, false, -1, true, false)
}

func TestConfChangeRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 20, false, true, false, -1, true, false)
}

func TestConfChangeUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, false, false, -1, true, false)
}

func TestConfChangeUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, -1, true, false)
}

func TestConfChangeSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, 100, true, false)
}

func TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, true, 100, true, false)
}

func TestOneSplit3B(t *testing.T) {
	cfg := config.NewTestConfig()
	cfg.RegionMaxSize = 800
	cfg.RegionSplitSize = 500
	cluster := NewTestCluster(5, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	cluster.MustPut([]byte("k1"), []byte("v1"))
	cluster.MustPut([]byte("k2"), []byte("v2"))

	region := cluster.GetRegion([]byte("k1"))
	region1 := cluster.GetRegion([]byte("k2"))
	assert.Equal(t, region.GetId(), region1.GetId())

	cluster.AddFilter(
		&PartitionFilter{
			s1: []uint64{1, 2, 3, 4},
			s2: []uint64{5},
		},
	)

	// write some data to trigger split
	for i := 100; i < 200; i++ {
		cluster.MustPut([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	time.Sleep(200 * time.Millisecond)
	cluster.ClearFilters()

	left := cluster.GetRegion([]byte("k1"))
	right := cluster.GetRegion([]byte("k2"))

	assert.NotEqual(t, left.GetId(), right.GetId())
	assert.True(t, bytes.Equal(region.GetStartKey(), left.GetStartKey()))
	assert.True(t, bytes.Equal(left.GetEndKey(), right.GetStartKey()))
	assert.True(t, bytes.Equal(right.GetEndKey(), region.GetEndKey()))

	req := NewRequest(left.GetId(), left.GetRegionEpoch(), []*raft_cmdpb.Request{NewGetCfCmd(engine_util.CfDefault, []byte("k2"))})
	resp, _ := cluster.CallCommandOnLeader(&req, time.Second)
	assert.NotNil(t, resp.GetHeader().GetError())
	assert.NotNil(t, resp.GetHeader().GetError().GetKeyNotInRegion())

	MustGetEqual(cluster.engines[5], []byte("k100"), []byte("v100"))
}

func TestSplitRecover3B(t *testing.T) {
	// Test: restarts, snapshots, conf change, one client (3B) ...
	GenericTest(t, "3B", 1, false, true, false, -1, false, true)
}

func TestSplitRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 20, false, true, false, -1, false, true)
}

func TestSplitUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, false, false, -1, false, true)
}

func TestSplitUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, -1, false, true)
}

func TestSplitConfChangeSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, 100, true, true)
}

func TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, conf change, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, true, 100, true, true)
}
