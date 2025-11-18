package scan

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go/support/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestScanTask(t *testing.T) {
	type want struct {
		high, low, count uint32
		gaps             []gap
	}
	cases := []struct {
		name    string
		batches [][]string
		part    task
		want    want
	}{
		{
			"full missing",
			[][]string{{}},
			task{low: 1, high: 100},
			want{0, 0, 0, []gap{{1, 100}}},
		},
		{
			"bottom-only gap",
			[][]string{{fpath(60, 100)}, {}},
			task{low: 1, high: 100},
			want{100, 60, 41, []gap{{1, 59}}},
		},
		{
			"contiguous coverage",
			[][]string{{fpath(50, 100)}, {fpath(1, 49)}, {}},
			task{low: 1, high: 100},
			want{100, 1, 100, []gap{}},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			ds := new(datastore.MockDataStore)
			for _, page := range c.batches {
				ds.On("ListFilePaths", mock.Anything, mock.Anything).Return(page, nil).Once()
			}
			schema := datastore.DataStoreSchema{LedgersPerFile: 64}

			res, err := scanTask(ctx, c.part, ds, schema)
			require.NoError(t, err)
			assert.Equal(t, c.want.high, res.high)
			assert.Equal(t, c.want.low, res.low)
			assert.Equal(t, c.want.count, res.count)
			assert.Equal(t, c.want.gaps, res.gaps)

			ds.AssertExpectations(t)
		})
	}
}

func TestScanTaskAdvanced(t *testing.T) {
	type expected struct {
		high, low, count uint32
		gaps             []gap
	}

	cases := []struct {
		name    string
		lpf     uint32
		part    task
		batches [][]string // pages returned by ListFilePaths in order
		want    expected
	}{
		{
			name: "Top+Internal+Bottom gaps",
			lpf:  64,
			part: task{low: 1, high: 100},
			batches: [][]string{
				{fpath(95, 98)}, // page 1
				{fpath(90, 92)}, // page 2
				{fpath(80, 89)}, // page 3
				{},              // page 4 (no more)
			},
			want: expected{
				high:  98,
				low:   80,
				count: (98 - 95 + 1) + (92 - 90 + 1) + (89 - 80 + 1),
				gaps: []gap{
					{Start: 99, End: 100}, // top
					{Start: 93, End: 94},  // internal
					{Start: 1, End: 79},   // bottom
				},
			},
		},
		{
			name: "Single-ledger-per-file with mixed gaps",
			lpf:  1,
			part: task{low: 1, high: 5},
			batches: [][]string{
				{spath(5)}, // first seen == high → no top gap
				{spath(3)}, // internal gap [4,4]
				{spath(1)}, // internal gap [2,2]; bottom covered
				{},         // end
			},
			want: expected{
				high:  5,
				low:   1,
				count: 3, // three single-ledger files
				gaps: []gap{
					{Start: 4, End: 4},
					{Start: 2, End: 2},
				},
			},
		},
		{
			name: "Multiple files in one page",
			lpf:  64,
			part: task{low: 1, high: 100},
			batches: [][]string{
				{spath(100), fpath(90, 95)}, // page 1: top covered; internal gap [96,99]
				{fpath(80, 89)},             // page 2
				{},                          // end → bottom gap [1,79]
			},
			want: expected{
				high:  100,
				low:   80,
				count: 1 + (95 - 90 + 1) + (89 - 80 + 1),
				gaps: []gap{
					{Start: 96, End: 99}, // internal
					{Start: 1, End: 79},  // bottom
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			ds := new(datastore.MockDataStore)

			for _, page := range c.batches {
				ds.On("ListFilePaths", mock.Anything, mock.Anything).
					Return(page, nil).Once()
			}

			schema := datastore.DataStoreSchema{LedgersPerFile: c.lpf}

			res, err := scanTask(ctx, c.part, ds, schema)
			require.NoError(t, err)

			assert.Equal(t, c.want.high, res.high, "high watermark")
			assert.Equal(t, c.want.low, res.low, "low watermark")
			assert.Equal(t, c.want.count, res.count, "ledger count")
			assert.Equal(t, c.want.gaps, res.gaps, "gaps")

			ds.AssertExpectations(t)
		})
	}
}

func TestScanTask_InvalidRange_Error(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)

	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{fpath(90, 100)}, nil).Once()
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{fpath(50, 40)}, nil).Once() // invalid
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{}, nil).Maybe()

	schema := datastore.DataStoreSchema{LedgersPerFile: 64}
	part := task{low: 1, high: 100}

	_, err := scanTask(ctx, part, ds, schema)
	require.Error(t, err)
	ds.AssertExpectations(t)
}

func TestScanTask_DatastoreError_BubblesUp(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)

	// First page OK, second page errors
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{fpath(95, 100)}, nil).Once()
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string(nil), assert.AnError).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 64}
	part := task{low: 1, high: 100}

	_, err := scanTask(ctx, part, ds, schema)
	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)

	ds.AssertExpectations(t)
}

func TestScanTask_ContextCanceledMidIteration(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)

	// Page 1 returns something; on the next "page" we cancel before Next() proceeds.
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{fpath(95, 100)}, nil).Once()
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string(nil), context.Canceled).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 64}
	part := task{low: 1, high: 100}

	_, err := scanTask(ctx, part, ds, schema)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	ds.AssertExpectations(t)
}

func spath(n uint32) string {
	return fmt.Sprintf("00000000--%d.xdr.zst", n)
}
