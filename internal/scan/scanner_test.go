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
			sc, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: c.lpf}, 3, c.inSize, nil)
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
	_, err := NewScanner(nil, datastore.DataStoreSchema{LedgersPerFile: 0}, 1, 16, nil)
	require.Error(t, err, "LPF=0 must be rejected")
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
			got := computePartitions(c.from, c.to, c.size)
			require.Equal(t, c.want, got)
		})
	}
}

func TestRun_InputValidation(t *testing.T) {
	sc := &Scanner{logger: log.DefaultLogger.WithField("t", "RunInput")}

	// from > to
	sc.numWorkers = 1
	sc.partitionSize = 10
	_, err := sc.Run(context.Background(), 10, 9)
	require.Error(t, err, "invalid range should error")

	// zero workers
	sc.numWorkers = 0
	sc.partitionSize = 10
	_, err = sc.Run(context.Background(), 1, 10)
	require.Error(t, err, "numWorkers == 0 should error")
}

func TestRun_HappyPath_CompletesAndCounts(t *testing.T) {
	// 1..25 with partitionSize=10 → [1–10], [11–20], [21–25]
	sc := &Scanner{
		numWorkers:    3,
		partitionSize: 10,
		logger:        log.DefaultLogger,
	}

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

func TestRun_FirstErrorCancelsOthers(t *testing.T) {
	sc := &Scanner{
		numWorkers:    4,
		partitionSize: 10,
		logger:        log.DefaultLogger,
	}

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
	assert.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, calls.Load(), int32(1))
}

func TestRun_ExternalCancelBeforeStart(t *testing.T) {
	sc := &Scanner{
		numWorkers:    8,
		partitionSize: 10,
		logger:        log.DefaultLogger,
	}
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
	sc := &Scanner{
		numWorkers:    4,
		partitionSize: 5,
		logger:        log.DefaultLogger,
	}
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
	sc := &Scanner{
		numWorkers:    64,
		partitionSize: 10,
		logger:        log.DefaultLogger,
	}
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
	sc := &Scanner{
		numWorkers:    3,
		partitionSize: 10,
		logger:        log.DefaultLogger,
	}
	var calls atomic.Int32
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		calls.Add(1)
		return Result{count: p.high - p.low + 1}, nil
	}

	_, err := sc.Run(context.Background(), 1, 30)
	require.NoError(t, err)
	assert.Equal(t, int32(3), calls.Load())
}
