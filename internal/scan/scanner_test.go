package scan

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func fpath(low, high uint32) string {
	return fmt.Sprintf("00000000--%d-%d.xdr.zst", low, high)
}

func TestNewScanner_NormalizesTaskSize(t *testing.T) {
	type tc struct {
		name     string
		lpf      uint32
		inSize   uint32
		wantSize uint32
	}
	cases := []tc{
		{"lt LPF -> LPF", 64, 1, 64},
		{"0 -> LPF", 64, 0, 64},
		{"round up", 64, 65, 128},
		{"exact multiple", 64, 128, 128},
		{"lpf=1 passthrough", 1, 5, 5},
		{"lpf large number", 1000, 1501, 2000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sc, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: c.lpf},
				3, c.inSize, log.DefaultLogger)
			if c.inSize == 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, c.wantSize, sc.taskSize)
			}
		})
	}
}

func TestNewScanner_InvalidLPF(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 0},
		1, 16, log.DefaultLogger)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid LedgersPerFile")
}

func TestNewScanner_InvalidWorkers(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 1},
		0, 16, log.DefaultLogger)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid worker count")
}

func TestNewScanner_InvalidTaskSize(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 1},
		1, 0, log.DefaultLogger)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid task size")
}

func TestNewScanner_NilLogger(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 0}, 1, 16, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid logger")
}

func TestComputeTasks(t *testing.T) {
	cases := []struct {
		name           string
		from, to, size uint32
		want           []task
	}{
		{"basic", 1, 10, 3, []task{{2, 3}, {4, 6},
			{7, 9}, {10, 10}}},
		{"exact multiple", 100, 115, 4, []task{{100, 103},
			{104, 107}, {108, 111}, {112, 115}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			scanner, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 1},
				1, c.size, log.DefaultLogger)
			require.NoError(t, err)
			got, err := scanner.computeTasks(c.from, c.to)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestRun_InputValidation(t *testing.T) {
	sc := &Scanner{}
	// from > to
	sc.numWorkers = 1
	sc.taskSize = 10
	_, err := sc.Run(context.Background(), 10, 9)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid range")
}

func newMockScanner(t *testing.T, numWorkers, taskSize uint32) *Scanner {
	t.Helper()

	ds := new(datastore.MockDataStore)
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{"00000000--1.xdr.zst"}, nil).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 1}
	sc, err := NewScanner(ds, schema, numWorkers, taskSize, log.DefaultLogger)
	require.NoError(t, err)

	// Teardown: verify all expectations were met.
	t.Cleanup(func() {
		ds.AssertExpectations(t)
	})

	return sc
}

func TestRun_HappyPath_WithGaps(t *testing.T) {
	t.Run("ledgers_per_file=1_with_gap", func(t *testing.T) {
		{
			sc := newMockScanner(t, 3, 10)
			sc.schema.LedgersPerFile = 1

			var total atomic.Uint64
			missingStart, missingEnd := uint32(5), uint32(7)

			sc.scan = func(ctx context.Context, p task) (result, error) {
				low := max(2, p.low)
				high := p.high
				if high < low {
					return result{low: p.low, high: p.high}, nil
				}

				presentCount := uint64(high - low + 1)
				var gs []Gap

				// subtract missing range overlap
				if high >= missingStart && low <= missingEnd {
					gStart := max(low, missingStart)
					gEnd := min(high, missingEnd)
					gs = gaps([2]uint32{gStart, gEnd})
					presentCount -= uint64(gEnd - gStart + 1)
				}

				total.Add(presentCount)
				return result{
					low:   low,
					high:  high,
					count: uint32(presentCount),
					gaps:  gs,
				}, nil
			}

			report, err := sc.Run(context.Background(), 0, 25)
			require.NoError(t, err)

			assert.Equal(t, uint64(24-3), total.Load())

			expected := Report{
				TotalLedgersFound:   24 - 3,
				TotalLedgersMissing: 3,
				MinSequenceFound:    2, // scanner normalizes the range to start at 2 since ledgers start from 2
				MaxSequenceFound:    25,
				Gaps:                gaps([2]uint32{5, 7}),
			}

			assert.Equal(t, expected, report)
		}
	})

	t.Run("ledgers_per_file>1_with_gap", func(t *testing.T) {
		{
			sc := newMockScanner(t, 3, 10)
			sc.schema.LedgersPerFile = 10

			var total atomic.Uint64
			// Missing the entire second file: 10–19
			missingStart := uint32(10)
			missingEnd := uint32(19)

			sc.scan = func(ctx context.Context, p task) (result, error) {
				low := max(2, p.low)
				high := p.high
				if high < low {
					return result{low: p.low, high: p.high}, nil
				}

				presentCount := uint64(high - low + 1)
				var gs []Gap

				// subtract missing range overlap
				if high >= missingStart && low <= missingEnd {
					gStart := min(low, missingStart)
					gEnd := min(high, missingEnd)
					gs = gaps([2]uint32{gStart, gEnd})
					presentCount -= uint64(gEnd - gStart + 1)
				}

				total.Add(presentCount)
				return result{
					low:   low,
					high:  high,
					count: uint32(presentCount),
					gaps:  gs,
				}, nil
			}

			report, err := sc.Run(context.Background(), 1, 25)
			require.NoError(t, err)

			// Full span = 28 ledgers (2–29)
			// Missing block = 10 ledgers (11–20)
			assert.Equal(t, uint64(28-10), total.Load())

			expected := Report{
				TotalLedgersFound:   28 - 10,
				TotalLedgersMissing: 10,
				MinSequenceFound:    2, // scanner normalizes the range to start at 2 since ledgers start from 2
				MaxSequenceFound:    29,
				Gaps:                gaps([2]uint32{10, 19}),
			}

			assert.Equal(t, expected, report)
		}
	})
}

func TestRun_HappyPath_NoGaps(t *testing.T) {
	t.Run("ledgers_per_file=1_no_gap", func(t *testing.T) {
		sc := newMockScanner(t, 3, 10)
		sc.schema.LedgersPerFile = 1

		var total atomic.Uint64
		sc.scan = func(ctx context.Context, p task) (result, error) {
			cnt := p.high - p.low + 1
			total.Add(uint64(cnt))
			return result{low: p.low, high: p.high, count: cnt}, nil
		}

		report, err := sc.Run(context.Background(), 0, 25)
		require.NoError(t, err)

		// With 1 ledger per file we scan 24 ledgers (2 - 25).
		assert.Equal(t, uint64(24), total.Load())

		expectedReport := Report{
			TotalLedgersFound:   24,
			TotalLedgersMissing: 0,
			MinSequenceFound:    2,
			MaxSequenceFound:    25,
		}
		assert.Equal(t, expectedReport, report)
	})

	t.Run("ledgers_per_file>1_no_gap", func(t *testing.T) {
		// Example: 10 ledgers per file.
		sc := newMockScanner(t, 3, 10)
		sc.schema.LedgersPerFile = 10

		var total atomic.Uint64
		sc.scan = func(ctx context.Context, p task) (result, error) {
			cnt := p.high - max(2, p.low) + 1
			total.Add(uint64(cnt))
			return result{low: p.low, high: p.high, count: cnt}, nil
		}

		report, err := sc.Run(context.Background(), 1, 25)
		require.NoError(t, err)

		// With 10 ledger per file we scan 28 ledgers (2 - 29).
		assert.Equal(t, uint64(28), total.Load())

		expectedReport := Report{
			TotalLedgersFound:   28,
			TotalLedgersMissing: 0,
			MinSequenceFound:    2,
			MaxSequenceFound:    29,
		}
		assert.Equal(t, expectedReport, report)
	})
}

func TestScannerRun_ReturnsFirstErrorAndPartialReport(t *testing.T) {
	ctx := context.Background()
	sc := newMockScanner(t, 1, 10)
	sc.schema.LedgersPerFile = 10

	errScan := fmt.Errorf("scan failed")

	// Override scan to control behavior:
	// - First task [2-9]: success, count=8
	// - Second task [10-19]: fails with errScan
	sc.scan = func(ctx context.Context, p task) (result, error) {
		switch {
		case p.low == 2 && p.high == 9:
			return result{
				gaps:  nil,
				low:   2,
				high:  9,
				count: 8,
				error: nil,
			}, nil
		case p.low == 10 && p.high == 19:
			return result{}, errScan
		case p.low == 20 && p.high == 29:
			return result{}, errScan
		default:
			t.Fatalf("unexpected scan task: %+v", p)
			return result{}, nil
		}
	}

	rep, gotErr := sc.Run(ctx, 1, 20)
	require.Error(t, gotErr)
	require.ErrorIs(t, gotErr, errScan)

	// We expect only the first task's data to be aggregated.
	assert.EqualValues(t, 8, rep.TotalLedgersFound)
	assert.EqualValues(t, 2, rep.MinSequenceFound)
	assert.EqualValues(t, 9, rep.MaxSequenceFound)
	assert.Len(t, rep.Gaps, 0)
}

func TestRun_FirstErrorCancelsOthers(t *testing.T) {
	sc := newMockScanner(t, 4, 10)

	var calls atomic.Int32
	sc.scan = func(ctx context.Context, p task) (result, error) {
		if calls.Add(1) == 1 {
			return result{error: assert.AnError}, assert.AnError // first task fails
		}
		// tiny delay to let cancel race be meaningful
		time.Sleep(5 * time.Millisecond)
		return result{count: 5}, nil
	}

	_, err := sc.Run(context.Background(), 1, 100)
	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
	assert.GreaterOrEqual(t, calls.Load(), int32(1))
}

func TestRun_ExternalCancelBeforeStart(t *testing.T) {
	sc := newMockScanner(t, 8, 10)
	sc.scan = func(ctx context.Context, p task) (result, error) {
		return result{count: p.high - p.low + 1}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := sc.Run(ctx, 1, 50)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRun_ExternalCancelMidway(t *testing.T) {
	sc := newMockScanner(t, 4, 5)

	sc.scan = func(ctx context.Context, p task) (result, error) {
		time.Sleep(10 * time.Millisecond) // simulate work so cancel can land
		return result{count: p.high - p.low + 1}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_, err := sc.Run(ctx, 1, 40)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRun_MoreWorkersThanTasks_Completes_NoDeadlock(t *testing.T) {
	// 1..10 with taskSize=10 => 1 task, but spin up 64 workers
	sc := newMockScanner(t, 64, 1)

	var calls atomic.Int32

	sc.scan = func(ctx context.Context, p task) (result, error) {
		calls.Add(1)
		return result{count: p.high - p.low + 1}, nil
	}

	_, err := sc.Run(context.Background(), 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int32(10), calls.Load())
}

func TestRun_CallsScanForEachTask(t *testing.T) {
	sc := newMockScanner(t, 3, 10)
	sc.schema.LedgersPerFile = 10

	var (
		mu    sync.Mutex
		lows  []uint32
		highs []uint32
		calls atomic.Int32
	)

	sc.scan = func(ctx context.Context, p task) (result, error) {
		mu.Lock()
		lows = append(lows, p.low)
		highs = append(highs, p.high)
		mu.Unlock()

		calls.Add(1)
		return result{count: p.high - p.low + 1}, nil
	}

	_, err := sc.Run(context.Background(), 1, 30)
	require.NoError(t, err)

	require.NotEmpty(t, lows)
	require.Equal(t, len(lows), len(highs))

	minN := lows[0]
	maxN := highs[0]
	for i := range lows {
		if lows[i] < minN {
			minN = lows[i]
		}
		if highs[i] > maxN {
			maxN = highs[i]
		}
	}

	// 1..30 with taskSize=10 and ledgersPerFile = 10 => 4 tasks (2-9, 10-19, 20-29, 30-39)
	assert.Equal(t, int32(4), calls.Load())
	assert.Equal(t, uint32(2), minN)
	assert.Equal(t, uint32(39), maxN)
}

func TestRun_DataStoreError(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{}, fmt.Errorf("boom")).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 10}
	sc, err := NewScanner(ds, schema, 4, 64, log.DefaultLogger)
	require.NoError(t, err)

	from, to := uint32(1), uint32(100)
	rep, err := sc.Run(ctx, from, to)
	require.Error(t, err)
	require.ErrorContains(t, err, "boom")
	require.Empty(t, rep)

	ds.AssertExpectations(t)
}

func TestRun_ShortCircuit_NoLedgerFiles(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{}, nil).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 10}
	sc, err := NewScanner(ds, schema, 4, 64, log.DefaultLogger)
	require.NoError(t, err)

	// Make sure the scanning path is never hit on short-circuit.
	sc.scan = func(ctx context.Context, p task) (result, error) {
		t.Fatalf("scan should not be called when datastore has no ledger files")
		return result{}, nil
	}

	from, to := uint32(2), uint32(100)

	rep, err := sc.Run(ctx, from, to)
	require.NoError(t, err)

	require.Len(t, rep.Gaps, 1)
	assert.Equal(t, Gap{Start: from, End: to}, rep.Gaps[0])
	assert.Equal(t, uint32(0), rep.TotalLedgersFound)
	assert.Equal(t, uint32(99), rep.TotalLedgersMissing)

	ds.AssertExpectations(t)
}

func TestComputeTasks_HighUint32Max(t *testing.T) {
	from := uint32(math.MaxUint32 - 50)
	to := uint32(math.MaxUint32)

	sc := &Scanner{
		taskSize: 1000,
		schema:   datastore.DataStoreSchema{LedgersPerFile: 1},
	}

	tasks, err := sc.computeTasks(from, to)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	got := tasks[0]
	require.Equal(t, from, got.low)
	require.Equal(t, to, got.high)
}
