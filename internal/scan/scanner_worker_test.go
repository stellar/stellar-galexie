package scan

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"
)

func TestWorker_ProcessesTask_EmitsResult(t *testing.T) {
	ctx := context.Background()
	resultsCh := make(chan result, 1)
	tasksCh := make(chan task, 1)

	sc := &Scanner{
		numWorkers: 1,
		logger:     log.DefaultLogger,
	}
	sc.scan = func(ctx context.Context, p task) (result, error) {
		return result{count: p.high - p.low + 1}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 0, resultsCh, tasksCh) }()

	tasksCh <- task{low: 10, high: 20}
	close(tasksCh)

	got := <-resultsCh
	require.Equal(t, uint32(11), got.count)
	require.Nil(t, got.error)
	wg.Wait()
}

func TestWorker_SetsErrorWhenScanFails(t *testing.T) {
	ctx := context.Background()
	resultsCh := make(chan result, 1)
	tasksCh := make(chan task, 1)

	sc := &Scanner{logger: log.DefaultLogger.WithField("t", "worker")}
	sc.scan = func(ctx context.Context, p task) (result, error) {
		return result{}, assert.AnError
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 1, resultsCh, tasksCh) }()

	tasksCh <- task{low: 1, high: 2}
	close(tasksCh)

	got := <-resultsCh
	require.Error(t, got.error)
	assert.ErrorIs(t, got.error, assert.AnError)
	wg.Wait()
}

func TestWorker_RespectsContextDoneBeforeAnyTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resultsCh := make(chan result, 1)
	tasks := make(chan task, 1)

	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p task) (result, error) { return result{}, nil }

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
	resultsCh := make(chan result, 1)
	tasks := make(chan task, 1)

	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p task) (result, error) { return result{}, nil }

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
	resultsCh := make(chan result)
	tasks := make(chan task, 1)
	ctx, cancel := context.WithCancel(context.Background())

	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p task) (result, error) { return result{count: 1}, nil }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 4, resultsCh, tasks) }()

	tasks <- task{low: 1, high: 1}
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
	resultsCh := make(chan result, 2)
	tasks := make(chan task, 2)

	var calls int
	sc := &Scanner{logger: log.DefaultLogger}
	sc.scan = func(ctx context.Context, p task) (result, error) {
		calls++
		return result{count: 1}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sc.worker(ctx, 5, resultsCh, tasks) }()

	p1 := task{low: 10, high: 19}
	p2 := task{low: 20, high: 29}
	tasks <- p1
	tasks <- p2
	close(tasks)

	<-resultsCh
	<-resultsCh
	wg.Wait()

	require.Equal(t, 2, calls)
}
