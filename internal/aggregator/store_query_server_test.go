// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/VKCOM/tl/pkg/rpc"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
)

// gatedQueryExecutor is a store-query executor the tests steer: every query
// waits on the release gate (or its context dying) and records the cause its
// context died with, so the tests observe exactly what the listener did to an
// in-flight query.
type gatedQueryExecutor struct {
	mu      sync.Mutex
	causes  []error // context.Cause of every ended query, in order
	started chan struct{}
	total   int // every query that ever reached the executor, waiting or not
	release chan struct{}
}

func newGatedQueryExecutor() *gatedQueryExecutor {
	return &gatedQueryExecutor{
		started: make(chan struct{}, 64),
		release: make(chan struct{}),
	}
}

func (f *gatedQueryExecutor) QuerySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	f.recordStarted()
	select {
	case <-f.release:
		return tlstatshouse.StoreSeriesResponse{}, nil
	case <-ctx.Done():
		f.recordCause(context.Cause(ctx))
		return tlstatshouse.StoreSeriesResponse{}, ctx.Err()
	}
}

func (f *gatedQueryExecutor) QueryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	f.recordStarted()
	select {
	case <-f.release:
		return tlstatshouse.StoreTagValuesResponse{}, nil
	case <-ctx.Done():
		f.recordCause(context.Cause(ctx))
		return tlstatshouse.StoreTagValuesResponse{}, ctx.Err()
	}
}

// recordStarted signals waitStarted and bumps the total, which — unlike the
// channel waitStarted drains — keeps counting every query that arrived.
func (f *gatedQueryExecutor) recordStarted() {
	f.mu.Lock()
	f.total++
	f.mu.Unlock()
	f.started <- struct{}{}
}

func (f *gatedQueryExecutor) recordCause(cause error) {
	f.mu.Lock()
	f.causes = append(f.causes, cause)
	f.mu.Unlock()
}

func (f *gatedQueryExecutor) endedCauses() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.causes...)
}

func (f *gatedQueryExecutor) startedTotal() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total
}

func (f *gatedQueryExecutor) openGate() { close(f.release) }

// startTestQueryServer serves store queries from executor over a real TCP
// loopback listener and returns a client dialed at it.
func startTestQueryServer(t *testing.T, executor storeQueryExecutor, concurrency int) *tlstatshouse.Client {
	t.Helper()
	_, cl := startTestQueryServerWithMetrics(t, executor, concurrency, nil)
	return cl
}

// startTestQueryServerWithMetrics is startTestQueryServer plus the admission
// recorder and the server handle, for the tests that steer or inspect the
// admission path itself.
func startTestQueryServerWithMetrics(t *testing.T, executor storeQueryExecutor, concurrency int, metrics storeQueryMetrics) (*storeQueryServer, *tlstatshouse.Client) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	srv := newStoreQueryServer(storeQueryServerConfig{
		Address:     ln.Addr().String(),
		Concurrency: concurrency,
		Metrics:     metrics,
	}, executor)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.CloseWait(ctx)
	})
	return srv, &tlstatshouse.Client{
		Client:  rpc.NewClient(rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion)),
		Network: "tcp4",
		Address: ln.Addr().String(),
	}
}

// waitStarted blocks until n queries have reached the executor.
func waitStarted(t *testing.T, f *gatedQueryExecutor, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-f.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for query %d of %d to reach the executor", i+1, n)
		}
	}
}

// requireErrorCode asserts err is a structured store error with exactly code.
func requireErrorCode(t *testing.T, err error, code int32, what string) {
	t.Helper()
	require.Error(t, err)
	got, ok := duckstore.ErrorCode(err)
	require.True(t, ok, "%s: the error must carry a structured store code, got %v", what, err)
	name, _ := duckstore.CodeName(got)
	require.Equal(t, code, got, "%s: wrong store error code (%s)", what, name)
}

// shrinkQueryQueueWait shortens the admission-wait ceiling so the refusal
// tests see their overloaded answer promptly instead of after the production
// 30 s.
func shrinkQueryQueueWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := storeQueryQueueWait
	storeQueryQueueWait = d
	t.Cleanup(func() { storeQueryQueueWait = old })
}

// shrinkQueryExecutionBudget shrinks the reserved execution budget the
// admission wait must leave the query, so tests with sub-second timeouts can
// still exercise waiting instead of being refused at once.
func shrinkQueryExecutionBudget(t *testing.T, d time.Duration) {
	t.Helper()
	old := storeQueryExecutionBudget
	storeQueryExecutionBudget = d
	t.Cleanup(func() { storeQueryExecutionBudget = old })
}

// The admission contract: two concurrent queries execute, the third waits for
// a slot for the (here shrunk) queue wait and is then refused as overloaded,
// and the admitted pair answer once their executor returns.
func TestStoreQueryServerThirdConcurrentQueryRefusedAsOverloaded(t *testing.T) {
	shrinkQueryQueueWait(t, 10*time.Millisecond)
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 2)

	const admitted = 2
	errs := make(chan error, admitted+1)
	for i := 0; i < admitted; i++ {
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			errs <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
	}
	waitStarted(t, f, admitted)

	// The two slots are busy, so this one must be refused promptly.
	refused := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreSeriesResponse
		refused <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	}()
	select {
	case err := <-refused:
		requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "third concurrent query")
	case <-time.After(5 * time.Second):
		t.Fatal("the third concurrent query was neither refused nor answered in time")
	}

	// A tag-values query is refused the same way: both verbs share admission.
	tvRefused := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreTagValuesResponse
		tvRefused <- cl.StoreQueryTagValues(context.Background(), tlstatshouse.StoreQueryTagValues{}, nil, &ret)
	}()
	select {
	case err := <-tvRefused:
		requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "third concurrent tag-values query")
	case <-time.After(5 * time.Second):
		t.Fatal("the third concurrent tag-values query was neither refused nor answered in time")
	}

	// Releasing the gate must free both slots and answer both clients.
	f.openGate()
	for i := 0; i < admitted; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("admitted query %d never answered", i)
		}
	}
	require.Empty(t, f.endedCauses(), "no admitted query should have been cancelled")
}

// The refusal path does not wait for admitted queries: once the (here
// shrunk) queue wait expires, the refusal arrives while the gate is still
// closed.
func TestStoreQueryServerRefusalDoesNotWaitForAdmittedQueries(t *testing.T) {
	shrinkQueryQueueWait(t, 10*time.Millisecond)
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 1)

	done := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreSeriesResponse
		done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	}()
	waitStarted(t, f, 1)

	start := time.Now()
	var ret tlstatshouse.StoreSeriesResponse
	err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "second concurrent query")
	require.Less(t, time.Since(start), time.Second, "the refusal must follow the queue wait promptly, not the running query")

	f.openGate()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted query never answered after the gate opened")
	}
}

// A burst beyond the slots must queue and drain, not collide: with one slot
// and the gate closed, three queries arrive together, one executes and two
// wait; opening the gate must run all three to completion. This is the
// dashboard shape — every tile firing at once against a shard whose slots
// are cheaper than the burst — and the reason admission waits instead of
// refusing on first sight of a busy slot.
func TestStoreQueryServerBurstQueuesAndDrainsThroughSlots(t *testing.T) {
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 1)

	const burst = 3
	errs := make(chan error, burst)
	for i := 0; i < burst; i++ {
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			errs <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
	}
	// Only the admitted one reaches the executor while the gate is closed;
	// the other two sit in the admission wait.
	waitStarted(t, f, 1)
	select {
	case err := <-errs:
		t.Fatalf("a queued query failed instead of waiting: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	f.openGate()
	for i := 0; i < burst; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err, "every burst query must answer once the gate opens")
		case <-time.After(5 * time.Second):
			t.Fatalf("burst query %d of %d never answered", i+1, burst)
		}
	}
	require.Empty(t, f.endedCauses(), "no burst query should have been cancelled")
}

// A client that drops while queued takes only its wait with it: the query
// never reaches the executor, no slot is consumed, and a later query still
// runs once the running one finishes.
//
// The waiter carries a short timeout_ms as the test's deterministic backstop:
// the client-side cancel returns before the server has necessarily processed
// it, but the server's own body timeout kills the wait at a known time — so
// by the time the gate opens (after that time) the waiter is provably gone
// and cannot take the slot the running query releases.
func TestStoreQueryServerCancelledWaiterTakesNoSlot(t *testing.T) {
	// The waiter's 300 ms body timeout must exceed the reserved execution
	// budget for the wait to run at all: shrink the budget so the waiter
	// parks instead of being refused at once.
	shrinkQueryExecutionBudget(t, 50*time.Millisecond)
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 1)

	// The running query holds the only slot.
	done := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreSeriesResponse
		done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	}()
	waitStarted(t, f, 1)

	// The waiter parks in the admission wait behind it; its client drops well
	// inside the 300 ms body timeout that bounds the wait server-side.
	start := time.Now()
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterArgs := tlstatshouse.StoreQuerySeries{}
	waiterArgs.Base.TimeoutMs = 300
	waiterDone := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreSeriesResponse
		waiterDone <- cl.StoreQuerySeries(waiterCtx, waiterArgs, nil, &ret)
	}()
	select {
	case err := <-waiterDone:
		t.Fatalf("the queued query gave up before its client cancelled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancelWaiter()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled, "the cancelled waiter must observe its own cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled waiter call never returned")
	}

	// Only open the gate once the waiter is dead server-side too — the cancel
	// may still be in flight, but the body timeout has fired for certain.
	time.Sleep(time.Until(start.Add(400 * time.Millisecond)))

	// The slot went to no one but the running query: a fresh query is
	// admitted the moment the gate opens, and the waiter never executed.
	f.openGate()
	var ret tlstatshouse.StoreSeriesResponse
	err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	require.NoError(t, err, "the freed slot must serve the next query, not the cancelled waiter")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the running query never answered after the gate opened")
	}
	require.Equal(t, 2, f.startedTotal(), "exactly two queries — the running one and the fresh one — must reach the executor")
}

// A client that cancels its RPC must actually stop the query: the executor
// sees the client-drop cause, nothing is sent, and the slot is freed for the
// next query.
func TestStoreQueryServerCancelledClientStopsQueryAndFreesSlot(t *testing.T) {
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreSeriesResponse
		done <- cl.StoreQuerySeries(ctx, tlstatshouse.StoreQuerySeries{}, nil, &ret)
	}()
	waitStarted(t, f, 1)

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled, "the client must observe its own cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled client call never returned")
	}

	// The query's context died with the client-drop cause, which is what
	// actually interrupted the executor. The client returning does not mean
	// the server side has processed the cancel yet, so wait for it.
	require.Eventually(t, func() bool { return len(f.endedCauses()) == 1 }, 5*time.Second, 10*time.Millisecond,
		"the cancelled query must stop on the server side")
	require.ErrorIs(t, f.endedCauses()[0], errStoreQueryCanceledByClient, "the query must die with the client-drop cause")

	// The slot must be free again: a fresh query is admitted and answered.
	f.openGate()
	var ret tlstatshouse.StoreSeriesResponse
	err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	require.NoError(t, err, "the admission slot must be released after the client cancelled")
}

// An elapsed timeout_ms from the request body maps onto the structured
// deadline_exceeded code, delivered to the still-waiting client.
func TestStoreQueryServerBodyTimeoutMapsToDeadlineExceeded(t *testing.T) {
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 1)

	args := tlstatshouse.StoreQuerySeries{}
	args.Base.TimeoutMs = 150

	// The watchdog context only guards the test: with no client timeout set,
	// the transport default (60s) never fires, so the body timeout is the
	// only deadline in play.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	var ret tlstatshouse.StoreSeriesResponse
	err := cl.StoreQuerySeries(ctx, args, nil, &ret)
	requireErrorCode(t, err, duckstore.ErrCodeDeadlineExceeded, "elapsed timeout_ms")
	require.Less(t, time.Since(start), 2*time.Second, "the answer must arrive at the body timeout, not the watchdog")

	// The query itself died with the transport-timeout cause.
	causes := f.endedCauses()
	require.Len(t, causes, 1)
	require.ErrorIs(t, causes[0], errStoreQueryTransportTimeout, "the query must die with the timeout cause")
}

// Both verbs answer through the longpoll path: the released gate produces
// responses the client can decode.
func TestStoreQueryServerTagValuesAnswered(t *testing.T) {
	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, DefaultQueryConcurrency)

	done := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreTagValuesResponse
		done <- cl.StoreQueryTagValues(context.Background(), tlstatshouse.StoreQueryTagValues{}, nil, &ret)
	}()
	waitStarted(t, f, 1)
	f.openGate()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the tag-values query never answered")
	}
}

// mapStoreQueryError classification: structured errors pass through, context
// failures classify by their cause, everything else becomes internal.
func TestMapStoreQueryError(t *testing.T) {
	// Structured store errors pass through untouched.
	structured := duckstore.NewError(duckstore.ErrCodeRowLimit, "too many rows")
	mapped := mapStoreQueryError(context.Background(), structured)
	require.Equal(t, structured, mapped)

	// A deadline that elapsed in the executor's own hands.
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	requireErrorCode(t, mapStoreQueryError(ctx, context.DeadlineExceeded), duckstore.ErrCodeDeadlineExceeded, "elapsed deadline")

	// An executor error that only makes sense once the cause is consulted:
	// the error says canceled, the cause says the transport deadline fired.
	for _, tc := range []struct {
		cause error
		code  int32
		what  string
	}{
		{errStoreQueryTransportTimeout, duckstore.ErrCodeDeadlineExceeded, "transport-deadline cause"},
		{context.DeadlineExceeded, duckstore.ErrCodeDeadlineExceeded, "parent deadline cause"},
		{errStoreQueryCanceledByClient, duckstore.ErrCodeCanceled, "client-drop cause"},
		{context.Canceled, duckstore.ErrCodeCanceled, "plain cancel cause"},
	} {
		ctx, cancel := context.WithCancelCause(context.Background())
		ctx, timeoutCancel := context.WithTimeoutCause(ctx, time.Hour, errStoreQueryTransportTimeout)
		defer timeoutCancel()
		cancel(tc.cause)
		requireErrorCode(t, mapStoreQueryError(ctx, context.Canceled), tc.code, tc.what)
	}

	// Anything else is internal: no raw error leaks as an unstructured one.
	requireErrorCode(t, mapStoreQueryError(context.Background(), fmt.Errorf("duckdb exploded")), duckstore.ErrCodeInternal, "opaque executor failure")
	require.NoError(t, mapStoreQueryError(context.Background(), nil))
}

// The longpoll classification: a WriteEmptyResponse after the transport
// deadline is a timeout, before it (a connection drop) is a client cancel.
func TestRunningStoreQueryWriteEmptyResponseClassification(t *testing.T) {
	classify := func(deadline time.Time) error {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer func() { cancel(nil) }()
		q := &runningStoreQuery{cancel: cancel, deadline: deadline}
		// The contract is to cancel and refuse to write any response.
		require.Equal(t, rpc.ErrLongpollNoEmptyResponse, q.WriteEmptyResponse(rpc.LongpollHandle{}, nil))
		<-ctx.Done()
		return context.Cause(ctx)
	}
	require.ErrorIs(t, classify(time.Now().Add(-time.Minute)), errStoreQueryTransportTimeout,
		"an empty response after the transport deadline means the deadline fired")
	require.ErrorIs(t, classify(time.Now().Add(time.Minute)), errStoreQueryCanceledByClient,
		"an empty response before the transport deadline means the client dropped")

	ctx, cancel := context.WithCancelCause(context.Background())
	defer func() { cancel(nil) }()
	q := &runningStoreQuery{cancel: cancel}
	q.CancelLongpoll(rpc.LongpollHandle{})
	require.ErrorIs(t, context.Cause(ctx), errStoreQueryCanceledByClient, "an explicit client cancel is a client cancel")
}

// timeout_ms interpretation: absent means the default, huge values clamp to
// the ceiling, everything in between is honoured verbatim.
func TestClampQueryTimeout(t *testing.T) {
	require.Equal(t, DefaultQueryTimeout, clampQueryTimeout(0))
	require.Equal(t, DefaultQueryTimeout, clampQueryTimeout(-5))
	require.Equal(t, 150*time.Millisecond, clampQueryTimeout(150))
	require.Equal(t, MaxQueryTimeout, clampQueryTimeout(int32(time.Hour/time.Millisecond)))
}

// The listener's own settings: bounded workers and a nonzero default response
// timeout, both derived from the admission slots — the opposite of the ingest
// listener's unlimited workers and absent timeouts.
func TestStoreQueryServerSettings(t *testing.T) {
	srv := newStoreQueryServer(storeQueryServerConfig{Address: "127.0.0.1:0"}, nil)
	require.Equal(t, DefaultQueryConcurrency, cap(srv.sema), "default admission tracks the machine's parallelism")
	require.Equal(t, queryWaiterFactor*DefaultQueryConcurrency, cap(srv.waiterSema), "the waiter bound is a factor of the admission slots")
	require.Positive(t, srv.workerLimit, "workers must be bounded, unlike the ingest listener's unlimited ones")
	require.Less(t, srv.workerLimit, 1<<20, "the worker bound must stay small, not wrap into unlimited")
	require.Equal(t, DefaultQueryTimeout, srv.defaultResponseTimeout, "the default response timeout must be nonzero")
	require.Equal(t, 30*time.Second, DefaultQueryQueueWait, "the raised wait ceiling: six times the old fixed 5 s wait")
	require.Equal(t, time.Second, DefaultQueryExecutionBudget, "one second of execution time is reserved after a wait")

	srv = newStoreQueryServer(storeQueryServerConfig{Address: "127.0.0.1:0", Concurrency: 5}, nil)
	require.Equal(t, 5, cap(srv.sema))
	require.Equal(t, queryWaiterFactor*5, cap(srv.waiterSema), "the waiter bound follows a reconfigured concurrency too")
	require.Equal(t, 13, srv.workerLimit, "the worker pool tracks the admission slots")
}

// The concurrency default is computed, not a constant: the machine's
// parallelism with a floor of two, so the smallest node still serves a
// dashboard's tiles in parallel while a bigger one follows its cores.
func TestDefaultQueryConcurrencyTracksGOMAXPROCS(t *testing.T) {
	require.GreaterOrEqual(t, DefaultQueryConcurrency, 2, "the floor of two survives any GOMAXPROCS")
	require.Equal(t, max(2, runtime.GOMAXPROCS(0)), DefaultQueryConcurrency, "the default is the machine's parallelism, floored at two")
}

// TestStoreQueryServerMalformedRequestIsBadRequest drives the parse-failure
// arm of both verbs directly — the TL client never produces undecodable
// bytes, so the mapping from a garbage request to the structured bad_request
// code is only reachable with a hand-built handler context, and a regression
// here would otherwise leak an unstructured rpc error to the API's fan-out.
func TestStoreQueryServerMalformedRequestIsBadRequest(t *testing.T) {
	s := newStoreQueryServer(storeQueryServerConfig{Concurrency: 1}, newGatedQueryExecutor())
	err := s.handleQuerySeries(context.Background(), &rpc.HandlerContext{Request: []byte{0xff, 0xff, 0xff}})
	requireErrorCode(t, err, duckstore.ErrCodeBadRequest, "garbage series request")
	err = s.handleQueryTagValues(context.Background(), &rpc.HandlerContext{Request: []byte{0x01, 0x02, 0x03}})
	requireErrorCode(t, err, duckstore.ErrCodeBadRequest, "garbage tag-values request")
}

// The admission wait runs toward the request's own clamped timeout but never
// past the absolute ceiling: with the (here shrunk) ceiling at 600 ms, a
// default-timeout query is refused at the ceiling — not instantly and not at
// its own 60 s — a short-timeout query's wait is cut by its own deadline
// instead, and a query whose slot frees inside the wait is still waiting and
// takes it.
func TestStoreQueryServerQueueWaitRunsTowardRequestTimeoutUnderCeiling(t *testing.T) {
	startRunning := func(t *testing.T, cl *tlstatshouse.Client, f *gatedQueryExecutor) chan error {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		waitStarted(t, f, 1)
		return done
	}

	t.Run("ceiling refuses a default-timeout query", func(t *testing.T) {
		shrinkQueryQueueWait(t, 600*time.Millisecond)
		f := newGatedQueryExecutor()
		cl := startTestQueryServer(t, f, 1)
		done := startRunning(t, cl, f)

		start := time.Now()
		var ret tlstatshouse.StoreSeriesResponse
		err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "query beyond the ceiling")
		waited := time.Since(start)
		require.GreaterOrEqual(t, waited, 500*time.Millisecond, "the refusal must follow the ceiling, not arrive at once")
		require.Less(t, waited, 3*time.Second, "the refusal must not wait for the running query, let alone the 60 s default timeout")

		f.openGate()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("the running query never answered after the gate opened")
		}
	})

	t.Run("request's own timeout cuts the wait below the ceiling", func(t *testing.T) {
		shrinkQueryQueueWait(t, 600*time.Millisecond)
		shrinkQueryExecutionBudget(t, 100*time.Millisecond)
		f := newGatedQueryExecutor()
		cl := startTestQueryServer(t, f, 1)
		done := startRunning(t, cl, f)

		// A 400 ms timeout leaves 300 ms of wait after its budget is
		// reserved: the refusal must arrive there — not at once, and not at
		// the 600 ms ceiling the default-timeout query of the arm above
		// waits out.
		args := tlstatshouse.StoreQuerySeries{}
		args.Base.TimeoutMs = 400
		start := time.Now()
		var ret tlstatshouse.StoreSeriesResponse
		err := cl.StoreQuerySeries(context.Background(), args, nil, &ret)
		requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "the short-timeout query, refused a budget ahead of its deadline")
		waited := time.Since(start)
		require.GreaterOrEqual(t, waited, 250*time.Millisecond, "the wait must run toward the request's timeout, not refuse at once")
		require.Less(t, waited, 450*time.Millisecond, "the wait must end a budget short of the request's timeout, well under the ceiling")

		f.openGate()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("the running query never answered after the gate opened")
		}
	})

	t.Run("slot freed inside the wait is taken", func(t *testing.T) {
		shrinkQueryQueueWait(t, 600*time.Millisecond)
		f := newGatedQueryExecutor()
		rec := &recordingAdmissions{}
		_, cl := startTestQueryServerWithMetrics(t, f, 1, rec)
		done := startRunning(t, cl, f)

		queued := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			queued <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		// The waiter is parked in the admission wait well inside both its 60 s
		// timeout and the 600 ms ceiling when the slot frees.
		time.Sleep(250 * time.Millisecond)
		f.openGate()
		select {
		case err := <-queued:
			require.NoError(t, err, "the waiter was still waiting and must take the freed slot")
		case <-time.After(5 * time.Second):
			t.Fatal("the queued query never answered")
		}
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("the running query never answered after the gate opened")
		}
		events := rec.snapshot()
		require.Len(t, events, 1, "the waiter's wait is the one admission outcome")
		require.Equal(t, storeQueryQueued, events[0].outcome)
		require.GreaterOrEqual(t, events[0].wait, 150*time.Millisecond, "it provably waited inside the wait window")
		require.Equal(t, 2, f.startedTotal(), "both queries must have reached the executor")
	})
}

// The execution-budget reservation: the admission wait ends a full budget
// short of the query's own deadline, so a query whose slot would only free
// inside that reserved window is refused as overloaded — never admitted with
// no useful time left to execute in — while the same slot freeing comfortably
// early is taken and the query runs.
func TestStoreQueryServerDeclinesSlotWithoutExecutionBudget(t *testing.T) {
	attempt := func(t *testing.T, openGateAfter time.Duration, timeoutMs int32) (*gatedQueryExecutor, *tlstatshouse.Client, error) {
		t.Helper()
		f := newGatedQueryExecutor()
		cl := startTestQueryServer(t, f, 1)
		done := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		waitStarted(t, f, 1)

		waiter := make(chan error, 1)
		args := tlstatshouse.StoreQuerySeries{}
		args.Base.TimeoutMs = timeoutMs
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			waiter <- cl.StoreQuerySeries(context.Background(), args, nil, &ret)
		}()
		time.Sleep(openGateAfter)
		f.openGate()
		select {
		case err := <-waiter:
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the running query never answered after the gate opened")
			}
			return f, cl, err
		case <-time.After(5 * time.Second):
			t.Fatal("the waiter never answered")
			return nil, nil, nil
		}
	}

	// A 2 s query whose slot only frees at ~1.3 s: the wait ended at 2 s minus
	// the 1 s budget, so the query was already refused — never admitted into
	// its reserved window, never executed.
	f, _, err := attempt(t, 1300*time.Millisecond, 2000)
	requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "a slot that only frees inside the execution budget")
	require.Equal(t, 1, f.startedTotal(), "the refused query must never reach the executor")

	// The same shape with the slot freeing at ~0.3 s: 1.7 s still to live,
	// over the budget — taken and executed.
	_, _, err = attempt(t, 300*time.Millisecond, 2000)
	require.NoError(t, err, "a slot freed with budget to spare must be taken")
}

// The waiter bound: the shard holds at most queryWaiterFactor queries per
// admission slot, waiting or executing; the query beyond the bound is refused
// at once — without waiting — and the held queries still drain through the
// slot once the gate opens. This is what keeps the longer admission wait from
// turning sustained overload into unbounded goroutines and longpoll entries.
func TestStoreQueryServerWaiterBoundShedsImmediately(t *testing.T) {
	shrinkQueryQueueWait(t, 2*time.Second)
	f := newGatedQueryExecutor()
	srv, cl := startTestQueryServerWithMetrics(t, f, 1, nil)

	done := make(chan error, 1)
	go func() {
		var ret tlstatshouse.StoreSeriesResponse
		done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	}()
	waitStarted(t, f, 1)

	// The running query holds a waiter token too, so one short of the bound
	// fits beside it.
	const waiting = queryWaiterFactor - 1
	errs := make(chan error, waiting)
	for i := 0; i < waiting; i++ {
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			errs <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
	}
	// Every waiter slot (the running one holds one too) is taken before the
	// query beyond the bound arrives, so what it meets is the bound, not a
	// race with a slow waiter.
	require.Eventually(t, func() bool { return len(srv.waiterSema) == cap(srv.waiterSema) },
		5*time.Second, 5*time.Millisecond, "the waiters must park in the bound first")

	start := time.Now()
	var ret tlstatshouse.StoreSeriesResponse
	err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "query beyond the waiter bound")
	require.Less(t, time.Since(start), time.Second, "the bound's refusal is immediate, not the 2 s queue wait")

	f.openGate()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the running query never answered after the gate opened")
	}
	for i := 0; i < waiting; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err, "every held query must drain through the slot")
		case <-time.After(5 * time.Second):
			t.Fatalf("held query %d of %d never answered", i+1, waiting)
		}
	}
	require.Equal(t, 1+waiting, f.startedTotal(), "every query the bound admitted must have executed")
}

// recordingAdmissions captures the admission outcomes the listener records,
// so the tests assert exactly which query-load events the listener itself
// reports: the executor only ever sees admitted queries, so refusals and
// waits are invisible without this recorder.
type recordingAdmissions struct {
	mu     sync.Mutex
	events []recordedAdmission
}

type recordedAdmission struct {
	verb    storeQueryVerb
	outcome storeQueryAdmission
	wait    time.Duration
}

func (r *recordingAdmissions) StoreQueryAdmission(verb storeQueryVerb, outcome storeQueryAdmission, wait time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, recordedAdmission{verb, outcome, wait})
}

func (r *recordingAdmissions) snapshot() []recordedAdmission {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedAdmission(nil), r.events...)
}

// The admission outcomes reach the recorder: a query shed at admission is
// reported as refused with its waited time, a query that waited and then ran
// as queued with its wait, a query its client cancelled while waiting is not
// reported at all (it was not shed), and a query shed by the waiter bound is
// refused with a zero wait.
func TestStoreQueryServerAdmissionOutcomesRecorded(t *testing.T) {
	t.Run("refused after the ceiling", func(t *testing.T) {
		shrinkQueryQueueWait(t, 20*time.Millisecond)
		f := newGatedQueryExecutor()
		rec := &recordingAdmissions{}
		_, cl := startTestQueryServerWithMetrics(t, f, 1, rec)
		done := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		waitStarted(t, f, 1)

		var ret tlstatshouse.StoreSeriesResponse
		err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "the shed query")
		events := rec.snapshot()
		require.Len(t, events, 1, "the refusal is the one admission outcome")
		require.Equal(t, storeQuerySeries, events[0].verb)
		require.Equal(t, storeQueryRefused, events[0].outcome)
		require.Positive(t, events[0].wait, "the refused query reports how long it waited")
	})

	t.Run("queued then executed, both verbs", func(t *testing.T) {
		shrinkQueryQueueWait(t, 500*time.Millisecond)
		f := newGatedQueryExecutor()
		rec := &recordingAdmissions{}
		_, cl := startTestQueryServerWithMetrics(t, f, 1, rec)
		done := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		waitStarted(t, f, 1)

		queued := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreTagValuesResponse
			queued <- cl.StoreQueryTagValues(context.Background(), tlstatshouse.StoreQueryTagValues{}, nil, &ret)
		}()
		time.Sleep(80 * time.Millisecond)
		f.openGate()
		select {
		case err := <-queued:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("the queued tag-values query never answered")
		}
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("the running query never answered after the gate opened")
		}
		events := rec.snapshot()
		require.Len(t, events, 1, "the wait is the one admission outcome")
		require.Equal(t, storeQueryTagValues, events[0].verb)
		require.Equal(t, storeQueryQueued, events[0].outcome)
		require.Positive(t, events[0].wait, "the queued query reports how long it waited")
	})

	t.Run("client cancel while waiting is not a refusal", func(t *testing.T) {
		shrinkQueryQueueWait(t, 2*time.Second)
		f := newGatedQueryExecutor()
		rec := &recordingAdmissions{}
		_, cl := startTestQueryServerWithMetrics(t, f, 1, rec)
		done := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		waitStarted(t, f, 1)

		// The waiter parks in the admission wait; its client cancels mid-wait.
		// A cancelled query was not shed by admission — it must not count as
		// a refusal.
		waiterCtx, cancelWaiter := context.WithCancel(context.Background())
		waiterDone := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			waiterDone <- cl.StoreQuerySeries(waiterCtx, tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		time.Sleep(80 * time.Millisecond)
		cancelWaiter()
		select {
		case err := <-waiterDone:
			require.ErrorIs(t, err, context.Canceled, "the cancelled waiter must observe its own cancellation")
		case <-time.After(5 * time.Second):
			t.Fatal("the cancelled waiter call never returned")
		}
		require.Empty(t, rec.snapshot(), "a query its client cancelled was not shed and must not count as a refusal")
	})

	t.Run("waiter-bound refusal is refused with zero wait", func(t *testing.T) {
		shrinkQueryQueueWait(t, 2*time.Second)
		f := newGatedQueryExecutor()
		rec := &recordingAdmissions{}
		srv, cl := startTestQueryServerWithMetrics(t, f, 1, rec)
		done := make(chan error, 1)
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			done <- cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
		waitStarted(t, f, 1)
		for i := 0; i < queryWaiterFactor-1; i++ { // the running query holds a waiter token too
			go func() {
				var ret tlstatshouse.StoreSeriesResponse
				_ = cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
			}()
		}
		require.Eventually(t, func() bool { return len(srv.waiterSema) == cap(srv.waiterSema) },
			5*time.Second, 5*time.Millisecond, "the waiters must park in the bound first")

		var ret tlstatshouse.StoreSeriesResponse
		err := cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "query beyond the waiter bound")
		events := rec.snapshot()
		require.Len(t, events, 1)
		require.Equal(t, storeQueryRefused, events[0].outcome)
		require.Zero(t, events[0].wait, "a query shed at once reports a zero wait")
	})
}
