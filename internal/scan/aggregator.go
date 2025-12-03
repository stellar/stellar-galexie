package scan

import (
	"sort"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// aggregator accumulates ledger scan results and merges contiguous Gap ranges.
type aggregator struct {
	logger     *log.Entry
	totalFound uint32
	minFound   uint32
	maxFound   uint32
	hasData    bool
	gaps       []Gap
}

// newAggregator creates a new, non-thread-safe aggregator.
// The caller is responsible for ensuring that only one goroutine calls Add()
// or Finalize() on a given instance.
func newAggregator(logger *log.Entry) *aggregator {
	return &aggregator{
		logger: logger,
	}
}

func (a *aggregator) add(res result) {
	if res.error != nil {
		return
	}

	a.gaps = append(a.gaps, res.gaps...)
	a.totalFound += res.count

	if res.count > 0 {
		if !a.hasData {
			a.minFound, a.maxFound, a.hasData = res.low, res.high, true
		} else {
			a.minFound = min(res.low, a.minFound)
			a.maxFound = max(res.high, a.maxFound)
		}
	}
}

func (a *aggregator) finalize() Report {
	// This should not occur in normal scans.
	// This state only happens if all tasks  are canceled
	// before producing any results.
	if !a.hasData && len(a.gaps) == 0 {
		return Report{}
	}

	finalGaps := sortAndMergeGaps(a.gaps)

	var totalMissing uint64
	for _, g := range finalGaps {
		totalMissing += uint64(g.End) - uint64(g.Start) + 1
	}

	if a.logger != nil {
		a.logger.WithFields(log.F{
			"found": a.totalFound,
			"gaps":  len(finalGaps),
		}).Info("report generation complete")
	}

	return Report{
		Gaps:                finalGaps,
		TotalLedgersFound:   a.totalFound,
		TotalLedgersMissing: uint32(totalMissing),
		MinSequenceFound:    a.minFound,
		MaxSequenceFound:    a.maxFound,
	}
}

func sortAndMergeGaps(gaps []Gap) []Gap {
	if len(gaps) <= 1 {
		return gaps
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].Start < gaps[j].Start })

	// Reuse backing array to avoid an extra alloc.
	merged := gaps[:0]
	current := gaps[0]

	for i := 1; i < len(gaps); i++ {
		next := gaps[i]
		curEnd := uint64(current.End)
		nextStart := uint64(next.Start)

		if nextStart <= curEnd+1 {
			// overlap or contiguous
			current.End = max(next.End, current.End)
		} else {
			merged = append(merged, current)
			current = next
		}
	}
	return append(merged, current)
}
