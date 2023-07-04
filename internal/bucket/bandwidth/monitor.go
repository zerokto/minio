// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package bandwidth

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type throttle struct {
	*rate.Limiter
	NodeBandwidthPerSec int64 // 每秒带宽
}

// Monitor holds the state of the global bucket monitor
// 监控器，
type Monitor struct {
	tlock                 sync.RWMutex                             // mutex for bucketThrottle
	bucketThrottle        map[string]map[string]*throttle          // 精确到Object的节流器
	mlock                 sync.RWMutex                             // mutex for activeBuckets map
	activeBuckets         map[string]map[string]*bucketMeasurement // Buckets with objects in flight
	bucketMovingAvgTicker *time.Ticker                             // Ticker for calculating moving averages
	ctx                   context.Context                          // Context for generate
	NodeCount             uint64
}

// NewMonitor returns a monitor with defaults.
func NewMonitor(ctx context.Context, numNodes uint64) *Monitor {
	m := &Monitor{
		activeBuckets:         make(map[string]map[string]*bucketMeasurement),
		bucketThrottle:        make(map[string]map[string]*throttle),
		bucketMovingAvgTicker: time.NewTicker(2 * time.Second),
		ctx:                   ctx,
		NodeCount:             numNodes,
	}
	// 定时计算
	go m.trackEWMA()
	return m
}

func (m *Monitor) updateMeasurement(bucket, arn string, bytes uint64) {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	tm, ok := m.activeBuckets[bucket]
	if !ok {
		tm = make(map[string]*bucketMeasurement)
	}
	measurement, ok := tm[arn]
	if !ok {
		measurement = &bucketMeasurement{}
	}
	measurement.incrementBytes(bytes)
	m.activeBuckets[bucket][arn] = measurement
}

// SelectionFunction for buckets
type SelectionFunction func(bucket string) bool

// SelectBuckets will select all the buckets passed in.
func SelectBuckets(buckets ...string) SelectionFunction {
	if len(buckets) == 0 {
		return func(bucket string) bool {
			return true
		}
	}
	return func(bucket string) bool {
		for _, b := range buckets {
			if b == "" || b == bucket {
				return true
			}
		}
		return false
	}
}

// Details for the measured bandwidth
type Details struct {
	LimitInBytesPerSecond            int64   `json:"limitInBits"`
	CurrentBandwidthInBytesPerSecond float64 `json:"currentBandwidth"`
}

// BucketBandwidthReport captures the details for all buckets.
type BucketBandwidthReport struct {
	BucketStats map[string]map[string]Details `json:"bucketStats,omitempty"`
}

// GetReport gets the report for all bucket bandwidth details.
// 获取所有存储桶和其目标的带宽使用情况报告。它使用getReport方法生成报告，
// 并使用提供的SelectionFunction来选择要包含在报告中的存储桶。
func (m *Monitor) GetReport(selectBucket SelectionFunction) *BucketBandwidthReport {
	m.mlock.RLock()
	defer m.mlock.RUnlock()
	return m.getReport(selectBucket)
}

func (m *Monitor) getReport(selectBucket SelectionFunction) *BucketBandwidthReport {
	report := &BucketBandwidthReport{
		BucketStats: make(map[string]map[string]Details),
	}
	for bucket, bucketMeasurementMap := range m.activeBuckets {
		if !selectBucket(bucket) {
			continue
		}
		m.tlock.RLock()
		report.BucketStats[bucket] = make(map[string]Details)
		if tgtThrottle, ok := m.bucketThrottle[bucket]; ok {
			for arn, throttle := range tgtThrottle {
				var currBw float64
				if bucketMeasurement, ok := bucketMeasurementMap[arn]; ok {
					currBw = bucketMeasurement.getExpMovingAvgBytesPerSecond()
				}
				report.BucketStats[bucket][arn] = Details{
					LimitInBytesPerSecond:            throttle.NodeBandwidthPerSec * int64(m.NodeCount),
					CurrentBandwidthInBytesPerSecond: currBw,
				}
			}
		}
		m.tlock.RUnlock()

	}
	return report
}

// EWMA 指数移动平均数算法
func (m *Monitor) trackEWMA() {
	for {
		select {
		case <-m.bucketMovingAvgTicker.C:
			// 定时更新窗口moving average
			m.updateMovingAvg()
		case <-m.ctx.Done():
			return
		}
	}
}

// 更新每个bucket的所有目标的EMA
func (m *Monitor) updateMovingAvg() {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	for _, bucketMeasurement := range m.activeBuckets {
		for _, measurement := range bucketMeasurement {
			measurement.updateExponentialMovingAverage(time.Now())
		}
	}
}

// 获取或建立bucketMeasurement
func (m *Monitor) getBucketMeasurement(bucket, arn string, initTime time.Time) map[string]*bucketMeasurement {
	bucketTracker, ok := m.activeBuckets[bucket]
	if !ok {
		bucketTracker = make(map[string]*bucketMeasurement)
		bucketTracker[arn] = newBucketMeasurement(initTime)
		m.activeBuckets[bucket] = bucketTracker
	}
	return bucketTracker
}

// track returns the measurement object for bucket
func (m *Monitor) track(bucket, arn string) {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	m.getBucketMeasurement(bucket, arn, time.Now())
}

// DeleteBucket deletes monitoring the 'bucket'
func (m *Monitor) DeleteBucket(bucket string) {
	m.tlock.Lock()
	delete(m.bucketThrottle, bucket)
	m.tlock.Unlock()
	m.mlock.Lock()
	delete(m.activeBuckets, bucket)
	m.mlock.Unlock()
}

// DeleteBucketThrottle deletes monitoring for a bucket's target
func (m *Monitor) DeleteBucketThrottle(bucket, arn string) {
	m.tlock.Lock()
	if _, ok := m.bucketThrottle[bucket]; ok {
		delete(m.bucketThrottle[bucket], arn)
	}
	m.tlock.Unlock()
	m.mlock.Lock()
	if _, ok := m.activeBuckets[bucket]; ok {
		delete(m.activeBuckets[bucket], arn)
	}
	m.mlock.Unlock()
}

// throttle returns currently configured throttle for this bucket
func (m *Monitor) throttle(bucket, arn string) *throttle {
	m.tlock.RLock()
	defer m.tlock.RUnlock()
	return m.bucketThrottle[bucket][arn]
}

// SetBandwidthLimit sets the bandwidth limit for a bucket
// 设置存储桶和其目标的带宽限制
func (m *Monitor) SetBandwidthLimit(bucket, arn string, limit int64) {
	m.tlock.Lock()
	defer m.tlock.Unlock()
	// 限制转成每个节点的限制
	bw := limit / int64(m.NodeCount)
	tgtMap, ok := m.bucketThrottle[bucket]
	if !ok {
		tgtMap = make(map[string]*throttle)
		tgtMap[arn] = &throttle{
			NodeBandwidthPerSec: bw,
		}
	}
	th, ok := tgtMap[arn]
	if !ok {
		th = &throttle{}
	}
	th.NodeBandwidthPerSec = bw
	tgtMap[arn] = th
	newlimit := rate.Every(time.Second / time.Duration(tgtMap[arn].NodeBandwidthPerSec))
	tgtMap[arn].Limiter = rate.NewLimiter(newlimit, int(tgtMap[arn].NodeBandwidthPerSec))
	m.bucketThrottle[bucket] = tgtMap
}

// IsThrottled returns true if a bucket has bandwidth throttling enabled.
func (m *Monitor) IsThrottled(bucket, arn string) bool {
	m.tlock.RLock()
	defer m.tlock.RUnlock()
	th, ok := m.bucketThrottle[bucket]
	if !ok {
		return ok
	}
	_, ok = th[arn]
	return ok
}
