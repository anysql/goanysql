package goanysql

import (
	"math/bits"
	"sync/atomic"
)

type ReadStatsItem struct {
	a_dayh uint64
	a_mins uint64
}

func (irs *ReadStatsItem) Touch(tm int64) {
	atomic.OrUint64(&irs.a_mins, (uint64(1) << ((tm / 60) % 60)))                                    // set current min
	atomic.OrUint64(&irs.a_dayh, (uint64(1)<<(32+(tm/3600)%24))|(uint64(1)<<((tm/86400)%32)))        // set current hour and day
	atomic.AndUint64(&irs.a_mins, ^(uint64(1) << ((tm/60 + 1) % 60)))                                // clear next min
	atomic.AndUint64(&irs.a_dayh, ^(uint64(1)<<(32+(tm/3600+1)%24))|^(uint64(1)<<((tm/86400+1)%32))) // clear next hour and day
}

func (irs *ReadStatsItem) ScoreMins() int {
	return 100 * bits.OnesCount64(irs.a_mins) / 60
}

func (irs *ReadStatsItem) ScoreHour() int {
	return 100 * bits.OnesCount64(irs.a_dayh>>32) / 24
}

func (irs *ReadStatsItem) ScoreDays() int {
	return 100 * bits.OnesCount64(irs.a_dayh<<32) / 32
}

func shardInode(inode uint64) uint32 {
	return uint32(inode * 11400714819323198485)
}

type FileReadStats struct {
	cache *LRUCache[uint64, ReadStatsItem]
}

func NewFileReadStats(capacity int) *FileReadStats {
	fsc := &FileReadStats{}
	fsc.cache = NewLRUCache[uint64, ReadStatsItem](16, capacity, shardInode)
	return fsc
}

func (fsc *FileReadStats) Touch(inode uint64, tm int64) {
	if entry, ok := fsc.cache.GetEntry(inode); ok {
		entry.Value.Touch(tm)
		entry.Hit = true
		return
	}
	var val ReadStatsItem
	val.Touch(tm)
	fsc.cache.Add(inode, val)
}

func (fsc *FileReadStats) ScoreMins(inode uint64) int {
	if entry, ok := fsc.cache.GetEntry(inode); ok {
		return entry.Value.ScoreMins()
	}
	return 0
}

func (fsc *FileReadStats) ScoreHour(inode uint64) int {
	if entry, ok := fsc.cache.GetEntry(inode); ok {
		return entry.Value.ScoreHour()
	}
	return 0
}

func (fsc *FileReadStats) ScoreDays(inode uint64) int {
	if entry, ok := fsc.cache.GetEntry(inode); ok {
		return entry.Value.ScoreDays()
	}
	return 0
}
