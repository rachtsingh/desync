package desync

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var debugStatsEnabled atomic.Bool

type debugStats struct {
	start time.Time

	localWalkEntries atomic.Int64
	localNextCalls   atomic.Int64
	localNextNs      atomic.Int64
	localXattrList   atomic.Int64
	localXattrGet    atomic.Int64
	localXattrNs     atomic.Int64
	localOpenFile    atomic.Int64
	localOpenFileNs  atomic.Int64
	localReadlink    atomic.Int64
	localReadlinkNs  atomic.Int64

	chunkerNextCalls atomic.Int64
	chunkerNextNs    atomic.Int64
	chunks           atomic.Int64
	chunkBytes       atomic.Int64
	workerBuildNs    atomic.Int64

	chunkStorageCalls         atomic.Int64
	chunkStorageNs            atomic.Int64
	chunkStorageProcessedHits atomic.Int64
	chunkStorageHasCalls      atomic.Int64
	chunkStorageHasHits       atomic.Int64
	chunkStorageHasMisses     atomic.Int64
	chunkStorageHasNs         atomic.Int64
	chunkStoragePutCalls      atomic.Int64
	chunkStoragePutNs         atomic.Int64

	s3StatCalls      atomic.Int64
	s3StatHits       atomic.Int64
	s3StatMisses     atomic.Int64
	s3StatNs         atomic.Int64
	s3PutCalls       atomic.Int64
	s3PutBytes       atomic.Int64
	s3PutNs          atomic.Int64
	s3GetCalls       atomic.Int64
	s3GetBytes       atomic.Int64
	s3GetNs          atomic.Int64
	s3InvalidRetries atomic.Int64
}

var globalDebugStats = &debugStats{}

// StartDebugStatsFromEnv starts periodic stderr counters when DESYNC_DEBUG_STATS is set.
func StartDebugStatsFromEnv(w io.Writer) func() {
	if os.Getenv("DESYNC_DEBUG_STATS") == "" {
		return func() {}
	}
	if w == nil {
		w = io.Discard
	}

	interval := 30 * time.Second
	if raw := os.Getenv("DESYNC_DEBUG_STATS_INTERVAL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			interval = d
		} else if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			interval = time.Duration(seconds) * time.Second
		}
	}

	globalDebugStats.start = time.Now()
	debugStatsEnabled.Store(true)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				globalDebugStats.print(w, "desync stats")
			case <-done:
				return
			}
		}
	}()

	return func() {
		debugStatsEnabled.Store(false)
		close(done)
		globalDebugStats.print(w, "desync final stats")
	}
}

func debugStatsActive() bool {
	return debugStatsEnabled.Load()
}

func debugStatsSince(start time.Time) int64 {
	if !debugStatsActive() {
		return 0
	}
	return time.Since(start).Nanoseconds()
}

func (s *debugStats) print(w io.Writer, label string) {
	elapsed := time.Since(s.start).Round(time.Second)
	fmt.Fprintf(w, "%s elapsed=%s fs_entries=%d fs_next=%d fs_next_time=%s xattr_list=%d xattr_get=%d xattr_time=%s open_files=%d open_time=%s readlinks=%d readlink_time=%s chunker_next=%d chunker_time=%s chunks=%d chunk_bytes=%s worker_build_time=%s store_calls=%d store_time=%s processed_hits=%d has_calls=%d has_hits=%d has_misses=%d has_time=%s put_calls=%d put_time=%s s3_stat_calls=%d s3_stat_hits=%d s3_stat_misses=%d s3_stat_time=%s s3_put_calls=%d s3_put_bytes=%s s3_put_time=%s s3_get_calls=%d s3_get_bytes=%s s3_get_time=%s s3_invalid_retries=%d\n",
		label,
		elapsed,
		s.localWalkEntries.Load(),
		s.localNextCalls.Load(),
		formatNs(s.localNextNs.Load()),
		s.localXattrList.Load(),
		s.localXattrGet.Load(),
		formatNs(s.localXattrNs.Load()),
		s.localOpenFile.Load(),
		formatNs(s.localOpenFileNs.Load()),
		s.localReadlink.Load(),
		formatNs(s.localReadlinkNs.Load()),
		s.chunkerNextCalls.Load(),
		formatNs(s.chunkerNextNs.Load()),
		s.chunks.Load(),
		formatBytes(s.chunkBytes.Load()),
		formatNs(s.workerBuildNs.Load()),
		s.chunkStorageCalls.Load(),
		formatNs(s.chunkStorageNs.Load()),
		s.chunkStorageProcessedHits.Load(),
		s.chunkStorageHasCalls.Load(),
		s.chunkStorageHasHits.Load(),
		s.chunkStorageHasMisses.Load(),
		formatNs(s.chunkStorageHasNs.Load()),
		s.chunkStoragePutCalls.Load(),
		formatNs(s.chunkStoragePutNs.Load()),
		s.s3StatCalls.Load(),
		s.s3StatHits.Load(),
		s.s3StatMisses.Load(),
		formatNs(s.s3StatNs.Load()),
		s.s3PutCalls.Load(),
		formatBytes(s.s3PutBytes.Load()),
		formatNs(s.s3PutNs.Load()),
		s.s3GetCalls.Load(),
		formatBytes(s.s3GetBytes.Load()),
		formatNs(s.s3GetNs.Load()),
		s.s3InvalidRetries.Load(),
	)
}

func formatNs(ns int64) string {
	return time.Duration(ns).Round(time.Millisecond).String()
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
