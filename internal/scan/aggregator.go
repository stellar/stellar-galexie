package scan

import (
	"sort"

	"github.com/stellar/go/support/log"
)

// aggregator accumulates ledger scan results and merges contiguous gap ranges.
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
		logger:   logger,
		minFound: ^uint32(0),
		gaps:     make([]Gap, 0, 1024), // reasonable default
	}
}

func (a *aggregator) add(res Result) {
	if res.error != nil {
		return
	}

	if len(res.gaps) > 0 {
		a.gaps = append(a.gaps, res.gaps...)
	}
	a.totalFound += res.count

	if res.count > 0 {
		if !a.hasData {
			a.minFound, a.maxFound, a.hasData = res.low, res.high, true
		} else {
			if res.low < a.minFound {
				a.minFound = res.low
			}
			if res.high > a.maxFound {
				a.maxFound = res.high
			}
		}
	}
}

func (a *aggregator) finalize() Report {
	if !a.hasData && len(a.gaps) == 0 {
		return Report{}
	}

	finalGaps := sortAndMergeGaps(a.gaps)

	var totalMissing uint64
	for _, g := range finalGaps {
		if g.Start <= g.End {
			totalMissing += uint64(g.End) - uint64(g.Start) + 1
		}
	}

	if a.logger != nil {
		a.logger.WithFields(log.F{
			"found": a.totalFound,
			"gaps":  len(finalGaps),
		}).Info("Report generation complete")
	}
	if !a.hasData {
		a.minFound = 0
		a.maxFound = 0
	}

	return Report{
		Gaps:         finalGaps,
		TotalFound:   a.totalFound,
		TotalMissing: totalMissing,
		Min:          a.minFound,
		Max:          a.maxFound,
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

		if nextStart <= curEnd+1 { // overlap or contiguous
			if next.End > current.End {
				current.End = next.End
			}
		} else {
			merged = append(merged, current)
			current = next
		}
	}
	return append(merged, current)
}
