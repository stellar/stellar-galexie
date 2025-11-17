package scan

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
)

func TestWorker_ProcessesTask_EmitsResult(t *testing.T) {
	ctx := context.Background()
	resultsCh := make(chan Result, 1)
	tasks := make(chan Partition, 1)

	sc := &Scanner{
		numWorkers: 1,
		logger:     log.DefaultLogger,
	}
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		return Result{count: p.high - p.low + 1}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 0, resultsCh, tasks) }()

	tasks <- Partition{low: 10, high: 20}
	close(tasks)

	got := <-resultsCh
	require.Equal(t, uint32(11), got.count)
	require.Nil(t, got.error)
	wg.Wait()
}

func TestWorker_SetsErrorWhenScanFails(t *testing.T) {
	ctx := context.Background()
	resultsCh := make(chan Result, 1)
	tasks := make(chan Partition, 1)

	sc := &Scanner{logger: log.DefaultLogger.WithField("t", "worker")}
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		return Result{}, assert.AnError
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 1, resultsCh, tasks) }()

	tasks <- Partition{low: 1, high: 2}
	close(tasks)

	got := <-resultsCh
	require.Error(t, got.error)
	assert.ErrorIs(t, got.error, assert.AnError)
	wg.Wait()
}

func TestWorker_RespectsContextDoneBeforeAnyTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resultsCh := make(chan Result, 1)
	tasks := make(chan Partition, 1)

	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p Partition) (Result, error) { return Result{}, nil }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 2, resultsCh, tasks) }()

	close(tasks)
	wg.Wait()

	select {
	case <-resultsCh:
		t.Fatalf("no result expected when context canceled before work")
	default:
	}
}

func TestWorker_ReturnsWhenTasksClosed(t *testing.T) {
	ctx := context.Background()
	resultsCh := make(chan Result, 1)
	tasks := make(chan Partition, 1)

	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p Partition) (Result, error) { return Result{}, nil }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 3, resultsCh, tasks) }()

	close(tasks)
	wg.Wait()

	select {
	case <-resultsCh:
		t.Fatalf("no result expected when no tasks were sent")
	default:
	}
}

func TestWorker_CanceledWhileSendingResult_DoesNotBlock(t *testing.T) {
	// Unbuffered results => send can block. We cancel ctx so worker exits via ctx.Done().
	resultsCh := make(chan Result)
	tasks := make(chan Partition, 1)
	ctx, cancel := context.WithCancel(context.Background())

	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p Partition) (Result, error) { return Result{count: 1}, nil }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 4, resultsCh, tasks) }()

	tasks <- Partition{low: 1, high: 1}
	time.Sleep(5 * time.Millisecond)
	cancel()
	close(tasks)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("worker did not exit after context " +
			"cancel while sending result")
	}

	// Drain if something slipped through.
	select {
	case <-resultsCh:
	default:
	}
}

func TestWorker_CallsScanForEachTask(t *testing.T) {
	ctx := context.Background()
	resultsCh := make(chan Result, 2)
	tasks := make(chan Partition, 2)

	var calls int
	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p Partition) (Result, error) {
		calls++
		return Result{count: 1}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 5, resultsCh, tasks) }()

	p1 := Partition{low: 10, high: 19}
	p2 := Partition{low: 20, high: 29}
	tasks <- p1
	tasks <- p2
	close(tasks)

	<-resultsCh
	<-resultsCh
	wg.Wait()

	require.Equal(t, 2, calls)
}
