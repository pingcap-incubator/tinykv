package raftstore

import (
	"os"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

func TestBootstrapStore(t *testing.T) {
	engines := newTestEngines(t)
	defer func() {
		os.RemoveAll(engines.KvPath)
		os.RemoveAll(engines.RaftPath)
	}()
	require.Nil(t, BootstrapStore(engines, 1, 1))
	require.NotNil(t, BootstrapStore(engines, 1, 1))
	_, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	region := new(metapb.Region)
	require.Nil(t, getMsg(engines.Kv, prepareBootstrapKey, region))
	_, err = getRegionLocalState(engines.Kv, 1)
	require.Nil(t, err)
	_, err = getApplyState(engines.Kv, 1)
	require.Nil(t, err)
	_, err = getRaftLocalState(engines.Raft, 1)
	require.Nil(t, err)

	require.Nil(t, ClearPrepareBootstrapState(engines))
	require.Nil(t, ClearPrepareBootstrap(engines, 1))
	empty, err := isRangeEmpty(engines.Kv, RegionMetaPrefixKey(1), RegionMetaPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)

	empty, err = isRangeEmpty(engines.Kv, RegionRaftPrefixKey(1), RegionRaftPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)
}
