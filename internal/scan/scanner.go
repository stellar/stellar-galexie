package scan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// Report represents the aggregated outcome of a full ledger scan.
// It summarizes all gaps, counts, and the overall ledger range discovered.
type Report struct {
	Gaps                []Gap  `json:"gaps,omitempty"`        // All missing ledger ranges found across the scan.
	TotalLedgersFound   uint32 `json:"total_ledgers_found"`   // Total number of ledgers successfully found.
	TotalLedgersMissing uint32 `json:"total_ledgers_missing"` // Total number of missing ledgers across all gaps.
	MinSequenceFound    uint32 `json:"min_sequence_found"`    // Lowest ledger sequence number observed.
	MaxSequenceFound    uint32 `json:"max_sequence_found"`    // Highest ledger sequence number observed.
}

// Gap represents a contiguous range of missing ledgers.
type Gap struct {
	Start uint32 `json:"start"` // First missing ledger in the range.
	End   uint32 `json:"end"`   // Last missing ledger in the range.
}

// task defines a contiguous range of ledger sequences
// that a single worker will scan.
type task struct {
	low  uint32 // Starting ledger sequence of the task.
	high uint32 // Ending ledger sequence of the task.
}

// result captures the outcome of a single scan task.
type result struct {
	gaps  []Gap  // Gaps found within this task.
	low   uint32 // Lowest ledger sequence found.
	high  uint32 // Highest ledger sequence found.
	count uint32 // Number of ledgers processed.
	error error  // Error encountered while running this task, if any.
}

// scanTaskFunc defines the function signature for scanning a ledger range.
// Allows injecting a mock implementation for testing.
type scanTaskFunc func(ctx context.Context, p task) (result, error)

// Scanner coordinates concurrent ledger scanning.
// It manages worker goroutines, distributes tasks, and aggregates results.
type Scanner struct {
	ds         datastore.DataStore
	schema     datastore.DataStoreSchema
	numWorkers uint32
	taskSize   uint32 // number of ledgers per task
	logger     *log.Entry
	scan       scanTaskFunc // injected; defaults to real scan
}

// NewScanner constructs a Scanner configured for parallel ledger scanning.
//
// It validates the task size relative to the schema’s LedgersPerFile setting,
// normalizes it to the nearest multiple of LedgersPerFile, and sets up the
// default scan implementation.
func NewScanner(store datastore.DataStore, schema datastore.DataStoreSchema,
	numWorkers uint32, taskSize uint32, logger *log.Entry) (*Scanner, error) {
	if logger == nil {
		return nil, fmt.Errorf("invalid logger: must not be nil")
	}

	lpf := schema.LedgersPerFile
	if lpf == 0 {
		return nil, fmt.Errorf("invalid LedgersPerFile (%d): must be greater than zero", lpf)
	}

	if taskSize == 0 {
		return nil, fmt.Errorf("invalid task size: must be greater than 0")
	}

	if numWorkers == 0 {
		return nil, fmt.Errorf("invalid worker count: must be at least 1")
	}

	// Normalize task size to a multiple of LedgersPerFile.
	effectiveSize := taskSize
	if effectiveSize < lpf {
		effectiveSize = lpf
	}
	effectiveSize = ((effectiveSize + lpf - 1) / lpf) * lpf

	sc := &Scanner{
		ds:         store,
		schema:     schema,
		numWorkers: numWorkers,
		taskSize:   effectiveSize,
		logger:     logger.WithField("subsystem", "scanner"),
	}

	// Default to the real scan implementation.
	sc.scan = func(ctx context.Context, p task) (result, error) {
		return scanTask(ctx, p, sc.ds, sc.schema)
	}
	return sc, nil
}

func (s *Scanner) worker(ctx context.Context, wid uint32, resultsCh chan result, tasksCh chan task) {
	for {
		select {
		case <-ctx.Done():
			return

		case p, ok := <-tasksCh:
			if !ok {
				// No more tasks.
				return
			}

			l := s.logger.WithFields(log.F{
				"worker_id": wid,
				"scan_from": p.low,
				"scan_to":   p.high,
			})

			l.Infof("worker_start [id=%d, range=%d-%d]", wid, p.low, p.high)

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
				l.WithFields(finishFields).WithError(err).
					Errorf("worker_failed [id=%d, range=%d-%d]", wid, p.low, p.high)
			} else {
				l.WithFields(finishFields).
					Infof("worker_finish [id=%d, range=%d-%d]", wid, p.low, p.high)
			}

			select {
			case resultsCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Run performs a full ledger scan over the range [from, to].
//
// The range is divided into scan tasks based on the configured task size,
// each processed concurrently by a worker. The first task error cancels
// all workers. A final aggregated report is returned.
func (s *Scanner) Run(ctx context.Context, from, to uint32) (Report, error) {
	if from > to {
		return Report{}, fmt.Errorf("invalid range: from=%d greater than to=%d", from, to)
	}

	// If the datastore contains no viable ledger files at all,
	// the entire range is missing and no scanning is needed.
	_, findErr := datastore.FindLatestLedgerUpToSequence(ctx, s.ds, to, s.schema)
	if findErr != nil {
		if errors.Is(findErr, datastore.ErrNoValidLedgerFiles) {
			missing := uint64(to) - uint64(from) + 1
			return Report{
				Gaps:                []Gap{{Start: from, End: to}},
				TotalLedgersFound:   0,
				TotalLedgersMissing: uint32(missing),
			}, nil
		}
		return Report{}, fmt.Errorf("datastore error: %w", findErr)
	}

	// Compute scan tasks using normalized task size.
	tasks, err := s.computeTasks(from, to)
	if err != nil {
		return Report{}, fmt.Errorf("failed to compute tasks for range [%d-%d]: %w", from, to, err)
	}

	// Use at most one worker per task.
	workers := min(s.numWorkers, uint32(len(tasks)))

	scanCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// Prepare tasks channel and prefill all scan tasks.
	tasksCh := make(chan task, len(tasks))
	for _, p := range tasks {
		select {
		case tasksCh <- p:
		case <-scanCtx.Done():
			close(tasksCh)
			return Report{}, scanCtx.Err()
		}
	}
	close(tasksCh)

	resultsCh := make(chan result, int(workers)*2)

	var wg sync.WaitGroup

	// Launch workers goroutines.
	for wid := uint32(0); wid < workers; wid++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			s.worker(scanCtx, id, resultsCh, tasksCh)
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
				return agg.finalize(), context.Cause(scanCtx)
			}

			if res.error != nil {
				cancel(res.error)
				continue
			}

			agg.add(res)

		case <-scanCtx.Done():
			return agg.finalize(), context.Cause(scanCtx)
		}
	}
}

func (s *Scanner) computeTasks(from, to uint32) ([]task, error) {
	if s.taskSize == 0 {
		return nil, fmt.Errorf("invalid task size: must be greater than 0")
	}

	// Align the outer range to ledger-file boundaries so that each task
	// starts and ends on a ledger boundary.
	start := s.schema.GetSequenceNumberStartBoundary(from)
	end := s.schema.GetSequenceNumberEndBoundary(to)

	if start > end {
		return nil, fmt.Errorf("invalid aligned range: start (%d) greater than end (%d)", start, end)
	}

	total := uint64(end) - uint64(start) + 1
	capacity := (total + uint64(s.taskSize) - 1) / uint64(s.taskSize)
	tasks := make([]task, 0, capacity)

	for low := start; low <= end; {
		high64 := uint64(low) + uint64(s.taskSize) - 1
		var high uint32
		if high64 > uint64(end) {
			high = end
		} else {
			high = uint32(high64)
		}
		tasks = append(tasks, task{low: max(2, low), high: high})
		if high == end {
			break
		}
		low = high + 1
	}
	return tasks, nil
}
