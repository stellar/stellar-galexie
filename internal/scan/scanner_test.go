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

func TestNewScanner_NormalizesPartitionSize(t *testing.T) {
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
				require.Equal(t, c.wantSize, sc.partitionSize)
			}
		})
	}
}

func TestNewScanner_InvalidLPF(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 0},
		1, 16, log.DefaultLogger)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid ledgersPerFile")
}

func TestNewScanner_InvalidWorkers(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 1},
		0, 16, log.DefaultLogger)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid worker count")
}

func TestNewScanner_InvalidPartitionSize(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 1},
		1, 0, log.DefaultLogger)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid partition size")
}

func TestNewScanner_NilLogger(t *testing.T) {
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 0}, 1, 16, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid logger")
}

func TestComputePartitions(t *testing.T) {
	cases := []struct {
		name           string
		from, to, size uint32
		want           []Partition
	}{
		{"basic", 1, 10, 3, []Partition{{1, 3}, {4, 6},
			{7, 9}, {10, 10}}},
		{"exact multiple", 100, 115, 4, []Partition{{100, 103},
			{104, 107}, {108, 111}, {112, 115}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			scanner, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 1},
				1, c.size, log.DefaultLogger)
			require.NoError(t, err)
			got, err := scanner.computePartitions(c.from, c.to)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestRun_InputValidation(t *testing.T) {
	sc := &Scanner{}
	// from > to
	sc.numWorkers = 1
	sc.partitionSize = 10
	_, err := sc.Run(context.Background(), 10, 9)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid range")
}

func newMockScanner(t *testing.T, numWorkers, partitionSize uint32) *Scanner {
	t.Helper()

	ds := new(datastore.MockDataStore)
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{"00000000--1-10.xdr.zst"}, nil).Once()
	ds.On("GetFileMetadata", mock.Anything, mock.Anything).
		Return(map[string]string{"end-ledger": "25"}, nil).Once()

	schema := datastore.DataStoreSchema{LedgersPerFile: 10}
	sc, err := NewScanner(ds, schema, numWorkers, partitionSize, log.DefaultLogger)
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
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		cnt := p.high - p.low + 1
		total.Add(uint64(cnt))
		return Result{count: cnt}, nil
	}

	_, err := sc.Run(context.Background(), 1, 25)
	require.NoError(t, err)
	assert.Equal(t, uint64(25), total.Load())
}

func TestScannerRun_ReturnsFirstErrorAndPartialReport(t *testing.T) {
	ctx := context.Background()
	sc := newMockScanner(t, 1, 10)

	errPartition := fmt.Errorf("partition failed")

	// Override scan to control behavior:
	// - First partition [1-10]: success, count=10
	// - Second partition [11-20]: fails with errPartition
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		switch {
		case p.low == 1 && p.high == 10:
			return Result{
				gaps:  nil,
				low:   1,
				high:  10,
				count: 10,
				error: nil,
			}, nil
		case p.low == 11 && p.high == 20:
			return Result{}, errPartition
		default:
			t.Fatalf("unexpected partition: %+v", p)
			return Result{}, nil
		}
	}

	report, gotErr := sc.Run(ctx, 1, 20)
	require.Error(t, gotErr)
	require.ErrorIs(t, gotErr, errPartition)

	// We expect only the first partition's data to be aggregated.
	assert.EqualValues(t, report.TotalFound, 10)
	assert.EqualValues(t, report.Min, 1)
	assert.EqualValues(t, report.Max, 10)
	assert.Len(t, report.Gaps, 0)
}

func TestRun_FirstErrorCancelsOthers(t *testing.T) {
	sc := newMockScanner(t, 4, 10)

	var calls atomic.Int32
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		if calls.Add(1) == 1 {
			return Result{error: assert.AnError}, assert.AnError // first partition fails
		}
		// tiny delay to let cancel race be meaningful
		time.Sleep(5 * time.Millisecond)
		return Result{count: 5}, nil
	}

	_, err := sc.Run(context.Background(), 1, 100)
	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
	assert.GreaterOrEqual(t, calls.Load(), int32(1))
}

func TestRun_ExternalCancelBeforeStart(t *testing.T) {
	sc := newMockScanner(t, 8, 10)
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		return Result{count: p.high - p.low + 1}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := sc.Run(ctx, 1, 50)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRun_ExternalCancelMidway(t *testing.T) {
	sc := newMockScanner(t, 4, 5)

	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		time.Sleep(10 * time.Millisecond) // simulate work so cancel can land
		return Result{count: p.high - p.low + 1}, nil
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

func TestRun_MoreWorkersThanPartitions_Completes_NoDeadlock(t *testing.T) {
	// 1..10 with partitionSize=10 => 1 partition, but spin up 64 workers
	sc := newMockScanner(t, 64, 10)

	var calls atomic.Int32

	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		calls.Add(1)
		return Result{count: p.high - p.low + 1}, nil
	}

	_, err := sc.Run(context.Background(), 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int32(1), calls.Load())
}

func TestRun_CallsScanForEachPartition(t *testing.T) {
	// 1..30 with partitionSize=10 => 3 partitions
	sc := newMockScanner(t, 3, 10)

	var calls atomic.Int32
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		calls.Add(1)
		return Result{count: p.high - p.low + 1}, nil
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
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		t.Fatalf("scan should not be called when datastore has no ledger files")
		return Result{}, nil
	}

	from, to := uint32(1), uint32(100)

	rep, err := sc.Run(ctx, from, to)
	require.NoError(t, err)

	require.Len(t, rep.Gaps, 1)
	assert.Equal(t, Gap{Start: from, End: to}, rep.Gaps[0])
	assert.Equal(t, uint32(0), rep.TotalFound)
	assert.Equal(t, uint64(to-from+1), rep.TotalMissing)

	ds.AssertExpectations(t)
}
