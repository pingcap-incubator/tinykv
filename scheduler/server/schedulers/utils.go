// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio             float64 = 0.005
	leaderTolerantSizeRatio float64 = 5.0
	minTolerantSizeRatio    float64 = 1.0
)

// ErrScheduleConfigNotExist the config is not correct.
var ErrScheduleConfigNotExist = errors.New("the config does not exist")

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func isRegionUnhealthy(region *core.RegionInfo) bool {
	return len(region.GetLearners()) != 0
}

func shouldBalance(cluster opt.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ScheduleKind, scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := source.GetID()
	targetID := target.GetID()
	tolerantResource := getTolerantResource(cluster, region, kind)
	sourceScore := source.ResourceScore(kind, -tolerantResource)
	targetScore := target.ResourceScore(kind, tolerantResource)

	// Make sure after move, source score is still greater than target score.
	shouldBalance := sourceScore > targetScore

	if !shouldBalance {
		log.Debug("skip balance "+kind.Resource.String(),
			zap.String("scheduler", scheduleName), zap.Uint64("region-id", region.GetID()), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", source.GetRegionSize()), zap.Float64("source-score", sourceScore),
			zap.Int64("target-size", target.GetRegionSize()), zap.Float64("target-score", targetScore),
			zap.Int64("average-region-size", cluster.GetAverageRegionSize()),
			zap.Int64("tolerant-resource", tolerantResource))
	}
	return shouldBalance
}

func getTolerantResource(cluster opt.Cluster, region *core.RegionInfo, kind core.ScheduleKind) int64 {
	if kind.Resource == core.LeaderKind {
		leaderCount := int64(1.0 * leaderTolerantSizeRatio)
		return leaderCount
	}

	regionSize := region.GetApproximateSize()
	if regionSize < cluster.GetAverageRegionSize() {
		regionSize = cluster.GetAverageRegionSize()
	}
	regionSize = int64(float64(regionSize) * getTolerantRatio(cluster))
	return regionSize
}

func getTolerantRatio(cluster opt.Cluster) float64 {
	var maxRegionCount float64
	stores := cluster.GetStores()
	for _, store := range stores {
		regionCount := float64(cluster.GetStoreRegionCount(store.GetID()))
		if maxRegionCount < regionCount {
			maxRegionCount = regionCount
		}
	}
	tolerantSizeRatio := maxRegionCount * adjustRatio
	if tolerantSizeRatio < minTolerantSizeRatio {
		tolerantSizeRatio = minTolerantSizeRatio
	}

	return tolerantSizeRatio
}
