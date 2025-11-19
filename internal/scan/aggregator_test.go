package scan

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper for concise gap literals
func gaps(gs ...[2]uint32) []gap {
	out := make([]gap, 0, len(gs))
	for _, g := range gs {
		out = append(out, gap{Start: g[0], End: g[1]})
	}
	return out
}

func TestAggregator_AddAndFinalize_Success(t *testing.T) {
	a := newAggregator(nil)

	// Add three results over a single range with internal gaps.
	a.add(result{gaps: gaps([2]uint32{20, 25}), low: 10, high: 19, count: 10})
	a.add(result{gaps: nil, low: 1, high: 9, count: 9})
	a.add(result{gaps: gaps([2]uint32{26, 30}), low: 21, high: 25, count: 5})

	rep := a.finalize()

	// Totals
	require.Equal(t, uint32(24), rep.TotalFound, "sum of counts should be 24")

	// MinFound/MaxFound across inputs
	require.Equal(t, uint32(1), rep.MinFound)
	require.Equal(t, uint32(25), rep.MaxFound)

	// gaps merged and accounted (20–25) + (26–30) → (20–30)
	require.Len(t, rep.Gaps, 1)
	assert.Equal(t, gap{Start: 20, End: 30}, rep.Gaps[0])

	// TotalMissing = inclusive size of [20,30] = 11
	assert.Equal(t, uint64(11), rep.TotalMissing)
}

func TestAggregator_Empty_ReturnsZeroReport(t *testing.T) {
	a := newAggregator(nil)
	rep := a.finalize()
	// Exactly zeroed report
	assert.Equal(t, report{}, rep)
}

func TestAggregator_gapsOnly_NoData_MinMax(t *testing.T) {
	a := newAggregator(nil)
	a.add(result{gaps: gaps([2]uint32{5, 7})}) // count==0, no low/high

	rep := a.finalize()
	require.Len(t, rep.Gaps, 1)
	assert.Equal(t, gap{Start: 5, End: 7}, rep.Gaps[0])
	assert.Equal(t, uint64(3), rep.TotalMissing)
	assert.Equal(t, uint32(0), rep.TotalFound)
	assert.Equal(t, uint32(0), rep.MinFound)
	assert.Equal(t, uint32(0), rep.MaxFound)
}

func TestAggregator_IgnoresErroredResults(t *testing.T) {
	a := newAggregator(nil)

	// Errored result should be ignored entirely.
	a.add(result{
		gaps:  gaps([2]uint32{100, 200}),
		low:   100,
		high:  200,
		count: 101,
		error: errors.New("boom"),
	})

	// Non-errored result should be accounted.
	a.add(result{
		gaps:  gaps([2]uint32{10, 12}),
		low:   1,
		high:  9,
		count: 9,
	})

	rep := a.finalize()
	require.Equal(t, uint32(9), rep.TotalFound)
	require.Equal(t, uint32(1), rep.MinFound)
	require.Equal(t, uint32(9), rep.MaxFound)

	require.Len(t, rep.Gaps, 1)
	assert.Equal(t, gap{Start: 10, End: 12}, rep.Gaps[0])
	assert.Equal(t, uint64(3), rep.TotalMissing)
}

func TestSortAndMergeGaps_UnsortedMergesCorrectly(t *testing.T) {
	in := []gap{
		{Start: 10, End: 20},
		{Start: 5, End: 9},   // contiguous with [10,20]
		{Start: 31, End: 31}, // contiguous with [18,30] after merging
		{Start: 100, End: 110},
		{Start: 18, End: 30}, // overlap with [10,20]
	}
	merged := sortAndMergeGaps(in)
	require.Len(t, merged, 2)
	assert.Equal(t, []gap{{Start: 5, End: 31}, {Start: 100, End: 110}}, merged)
}

func TestSortAndMergeGaps_DisjointRemainSeparate(t *testing.T) {
	in := []gap{
		{Start: 1, End: 3},
		{Start: 5, End: 6},
		{Start: 8, End: 9},
	}
	merged := sortAndMergeGaps(in)
	require.Len(t, merged, 3)
	assert.Equal(t, []gap{
		{Start: 1, End: 3},
		{Start: 5, End: 6},
		{Start: 8, End: 9},
	}, merged)
}

func TestSortAndMergeGaps_SingleAndMaxUint32(t *testing.T) {
	one := []gap{{Start: 42, End: 42}}
	assert.Equal(t, one, sortAndMergeGaps(one))

	max := []gap{{Start: 0, End: ^uint32(0)}}
	m := sortAndMergeGaps(max)
	require.Len(t, m, 1)
	assert.Equal(t, uint32(0), m[0].Start)
	assert.Equal(t, ^uint32(0), m[0].End)
}

func TestSortAndMergeGaps_AlreadyMerged_NoChange(t *testing.T) {
	in := []gap{
		{Start: 1, End: 10},
		{Start: 12, End: 20},
	}
	merged := sortAndMergeGaps(in)
	require.Len(t, merged, 2)
	assert.Equal(t, in, merged)
}
