package scan

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
)

// Report represents the aggregated outcome of a full ledger scan.
// It summarizes all gaps, counts, and the overall ledger range discovered.
type Report struct {
	Gaps         []Gap  `json:"gaps,omitempty"` // All missing ledger ranges found across partitions.
	TotalFound   uint32 `json:"total_found"`    // Total number of ledgers successfully found.
	TotalMissing uint64 `json:"total_missing"`  // Total number of missing ledgers across all gaps.
	Min          uint32 `json:"min"`            // Lowest ledger sequence number observed.
	Max          uint32 `json:"max"`            // Highest ledger sequence number observed.
}

// Gap represents a contiguous range of missing ledgers detected
// between existing data ranges.
type Gap struct {
	Start uint32 `json:"start"` // First missing ledger in the range.
	End   uint32 `json:"end"`   // Last missing ledger in the range.
}

// Partition defines a contiguous range of ledger sequences
// that should be scanned together by one worker.
type Partition struct {
	low  uint32 // Starting ledger sequence of the partition.
	high uint32 // Ending ledger sequence of the partition.
}

// Result captures the outcome of scanning a single partition.
type Result struct {
	gaps  []Gap  // Gaps found within this partition.
	low   uint32 // Lowest ledger sequence found.
	high  uint32 // Highest ledger sequence found.
	count uint32 // Number of ledgers processed.
	error error  // Error encountered while scanning this partition, if any.
}

// scanPartitionFunc defines the function signature for scanning a partition.
// It allows the Scanner to inject a custom implementation for testing
// or to use the default `scanPartition` function in production.
type scanPartitionFunc func(ctx context.Context, p Partition) (Result, error)

// Scanner coordinates the concurrent scanning of ledger partitions.
// It manages worker routines, partitions, and aggregates results.
type Scanner struct {
	ds            datastore.DataStore
	schema        datastore.DataStoreSchema
	numWorkers    uint32
	partitionSize uint32
	logger        *log.Entry
	scan          scanPartitionFunc // injected; defaults to real scanPartition
}

// NewScanner constructs a new Scanner configured for parallel ledger scanning.
//
// It validates and normalizes the partition size relative to the schema’s
// LedgersPerFile setting. The function also assigns the real scanPartition
// implementation by default, but the `scan` field can be overridden for tests.
func NewScanner(store datastore.DataStore, schema datastore.DataStoreSchema,
	numWorkers uint32, partitionSize uint32, logger *log.Entry) (*Scanner, error) {
	lpf := schema.LedgersPerFile
	if lpf == 0 {
		return nil, fmt.Errorf("invalid ledgersPerFile [%d]: must be greater than zero", lpf)
	}

	if partitionSize == 0 {
		return nil, fmt.Errorf("partitionSize must be > 0")
	}

	if numWorkers == 0 {
		return nil, fmt.Errorf("numWorkers must be > 0")
	}

	// Default a nil logger to the package default
	if logger == nil {
		logger = log.DefaultLogger
	}

	// Calculate the effective partition size
	effectiveSize := partitionSize

	// Ensure effectiveSize is at least one LPF
	if effectiveSize < lpf {
		effectiveSize = lpf
	}

	// Round effectiveSize up to the nearest multiple of lpf
	effectiveSize = ((effectiveSize + lpf - 1) / lpf) * lpf

	sc := &Scanner{
		ds:            store,
		schema:        schema,
		numWorkers:    numWorkers,
		partitionSize: effectiveSize,
		logger:        logger,
	}

	// Default to real implementation
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		return scanPartition(ctx, p, sc.ds, sc.schema)
	}
	return sc, nil
}

func (s *Scanner) worker(ctx context.Context, wid uint32, resultsCh chan Result, tasks chan Partition) {
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-tasks:
			if !ok {
				// tasks closed: nothing left to do
				return
			}

			l := s.logger.WithFields(log.F{
				"worker_id": wid,
				"scan_from": p.low,
				"scan_to":   p.high,
			})

			l.Infof("worker_start [WID:%d, RANGE:%d-%d]", wid, p.low, p.high)

			start := time.Now()
			res, err := s.scan(ctx, p)
			dur := time.Since(start)

			finishFields := log.F{
				"ledgers_found": res.count,
				"gaps":          len(res.gaps),
				"duration_ms":   dur.Milliseconds(),
			}

			if err != nil {
				res.error = err
				l.WithFields(finishFields).WithError(err).Errorf("worker_failed [WID:%d, RANGE:%d-%d]", wid, p.low, p.high)
			} else {
				l.WithFields(finishFields).Infof("worker_finish [WID:%d, RANGE:%d-%d]", wid, p.low, p.high)
			}

			select {
			case resultsCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Run executes a full ledger scan across the range [from, to], coordinating
// multiple worker goroutines and aggregating their results into a final Report.
//
// The scan is divided into partitions based on the scanner’s configured
// partition size, and each partition is processed concurrently by workers.
// The method will cancel all workers on the first error encountered or if the
// provided context is canceled.
func (s *Scanner) Run(ctx context.Context, from, to uint32) (Report, error) {
	if from > to {
		return Report{}, fmt.Errorf("invalid range")
	}

	if s.partitionSize == 0 {
		return Report{}, fmt.Errorf("partitionSize must be > 0")
	}

	// Compute scan partitions using normalized partition size.
	parts := computePartitions(from, to, s.partitionSize)
	if len(parts) > math.MaxUint32 {
		return Report{}, fmt.Errorf("too many partitions")
	}

	// Use at most one worker per partition.
	workers := s.numWorkers
	if workers > uint32(len(parts)) {
		workers = uint32(len(parts))
	}
	if workers == 0 {
		return Report{}, fmt.Errorf("numWorkers must be >= 1")
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Prepare tasks channel and prefill all partitions.
	tasks := make(chan Partition, len(parts))
	for _, p := range parts {
		select {
		case tasks <- p:
		case <-scanCtx.Done():
			close(tasks)
			return Report{}, scanCtx.Err()
		}
	}
	close(tasks)

	// resultsCh collects results from workers.
	resultsCh := make(chan Result, int(workers)*2)

	var wg sync.WaitGroup

	// Launch workers goroutines.
	for wid := uint32(0); wid < workers; wid++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			s.worker(scanCtx, id, resultsCh, tasks)
		}(wid)
	}

	// Close results after all workers finish.
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	agg := newAggregator(s.logger)

	for {
		select {
		case res, ok := <-resultsCh:
			if !ok {
				return agg.finalize(), nil
			}
			if res.error != nil {
				// Stop remaining work on first error.
				cancel()
			}
			agg.add(res)

		case <-scanCtx.Done():
			// Propagate cancellation cause.
			return agg.finalize(), scanCtx.Err()
		}
	}
}

func computePartitions(from, to, partitionSize uint32) []Partition {
	capacity := (to-from)/partitionSize + 1
	partitions := make([]Partition, 0, capacity)

	for low := from; low <= to; {
		// Calculate the end of the partition
		high := low + partitionSize - 1

		// Truncate if it goes past the overall 'to' boundary
		if high > to {
			high = to
		}

		//	low = max(2, low)
		partitions = append(partitions, Partition{low: low, high: high})
		low = high + 1
	}
	return partitions
}
