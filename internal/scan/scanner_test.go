package scan

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
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
		{"basic", 1, 10, 3, []task{{1, 3}, {4, 6},
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
		Return([]string{"00000000--1-10.xdr.zst"}, nil).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 10}
	sc, err := NewScanner(ds, schema, numWorkers, taskSize, log.DefaultLogger)
	require.NoError(t, err)

	// Teardown: verify all expectations were met.
	t.Cleanup(func() {
		ds.AssertExpectations(t)
	})

	return sc
}

func TestRun_HappyPath_CompletesAndCounts(t *testing.T) {
	sc := newMockScanner(t, 3, 10)

	var total atomic.Uint64
	sc.scan = func(ctx context.Context, p task) (result, error) {
		cnt := p.high - p.low + 1
		total.Add(uint64(cnt))
		return result{count: cnt}, nil
	}

	_, err := sc.Run(context.Background(), 1, 25)
	require.NoError(t, err)
	assert.Equal(t, uint64(25), total.Load())
}

func TestScannerRun_ReturnsFirstErrorAndPartialReport(t *testing.T) {
	ctx := context.Background()
	sc := newMockScanner(t, 1, 10)

	errScan := fmt.Errorf("scan failed")

	// Override scan to control behavior:
	// - First task [1-10]: success, count=10
	// - Second task [11-20]: fails with errScan
	sc.scan = func(ctx context.Context, p task) (result, error) {
		switch {
		case p.low == 1 && p.high == 10:
			return result{
				gaps:  nil,
				low:   1,
				high:  10,
				count: 10,
				error: nil,
			}, nil
		case p.low == 11 && p.high == 20:
			return result{}, errScan
		default:
			t.Fatalf("unexpected scan task: %+v", p)
			return result{}, nil
		}
	}

	report, gotErr := sc.Run(ctx, 1, 20)
	require.Error(t, gotErr)
	require.ErrorIs(t, gotErr, errScan)

	// We expect only the first task's data to be aggregated.
	assert.EqualValues(t, report.TotalFound, 10)
	assert.EqualValues(t, report.MinFound, 1)
	assert.EqualValues(t, report.MaxFound, 10)
	assert.Len(t, report.Gaps, 0)
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
	sc := newMockScanner(t, 64, 10)

	var calls atomic.Int32

	sc.scan = func(ctx context.Context, p task) (result, error) {
		calls.Add(1)
		return result{count: p.high - p.low + 1}, nil
	}

	_, err := sc.Run(context.Background(), 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int32(1), calls.Load())
}

func TestRun_CallsScanForEachTask(t *testing.T) {
	// 1..30 with taskSize=10 => 3 task
	sc := newMockScanner(t, 3, 10)

	var calls atomic.Int32
	sc.scan = func(ctx context.Context, p task) (result, error) {
		calls.Add(1)
		return result{count: p.high - p.low + 1}, nil
	}

	_, err := sc.Run(context.Background(), 1, 30)
	require.NoError(t, err)
	assert.Equal(t, int32(3), calls.Load())
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

	from, to := uint32(1), uint32(100)

	rep, err := sc.Run(ctx, from, to)
	require.NoError(t, err)

	require.Len(t, rep.Gaps, 1)
	assert.Equal(t, gap{Start: from, End: to}, rep.Gaps[0])
	assert.Equal(t, uint32(0), rep.TotalFound)
	assert.Equal(t, uint64(to-from+1), rep.TotalMissing)

	ds.AssertExpectations(t)
}
