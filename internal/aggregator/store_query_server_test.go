// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"fmt"
	"net"
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
	release chan struct{}
}

func newGatedQueryExecutor() *gatedQueryExecutor {
	return &gatedQueryExecutor{
		started: make(chan struct{}, 64),
		release: make(chan struct{}),
	}
}

func (f *gatedQueryExecutor) QuerySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	f.started <- struct{}{}
	select {
	case <-f.release:
		return tlstatshouse.StoreSeriesResponse{}, nil
	case <-ctx.Done():
		f.mu.Lock()
		f.causes = append(f.causes, context.Cause(ctx))
		f.mu.Unlock()
		return tlstatshouse.StoreSeriesResponse{}, ctx.Err()
	}
}

func (f *gatedQueryExecutor) QueryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	f.started <- struct{}{}
	select {
	case <-f.release:
		return tlstatshouse.StoreTagValuesResponse{}, nil
	case <-ctx.Done():
		f.mu.Lock()
		f.causes = append(f.causes, context.Cause(ctx))
		f.mu.Unlock()
		return tlstatshouse.StoreTagValuesResponse{}, ctx.Err()
	}
}

func (f *gatedQueryExecutor) endedCauses() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.causes...)
}

func (f *gatedQueryExecutor) openGate() { close(f.release) }

// startTestQueryServer serves store queries from executor over a real TCP
// loopback listener and returns a client dialed at it.
func startTestQueryServer(t *testing.T, executor storeQueryExecutor, concurrency int) *tlstatshouse.Client {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	srv := newStoreQueryServer(storeQueryServerConfig{
		Address:     ln.Addr().String(),
		Concurrency: concurrency,
	}, executor)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.CloseWait(ctx)
	})
	return &tlstatshouse.Client{
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

// The admission contract: two concurrent queries execute, the third is refused
// as overloaded instead of queueing, and the admitted pair answer once their
// executor returns.
func TestStoreQueryServerThirdConcurrentQueryRefusedAsOverloaded(t *testing.T) {
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

// The refusal path is immediate: a refused query must not wait for the gate.
func TestStoreQueryServerRefusalDoesNotWaitForAdmittedQueries(t *testing.T) {
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
	require.Less(t, time.Since(start), time.Second, "refusal must be immediate, not queued behind the running query")

	f.openGate()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted query never answered after the gate opened")
	}
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
	require.Equal(t, DefaultQueryConcurrency, cap(srv.sema), "default admission is two slots")
	require.Positive(t, srv.workerLimit, "workers must be bounded, unlike the ingest listener's unlimited ones")
	require.Less(t, srv.workerLimit, 1<<20, "the worker bound must stay small, not wrap into unlimited")
	require.Equal(t, DefaultQueryTimeout, srv.defaultResponseTimeout, "the default response timeout must be nonzero")

	srv = newStoreQueryServer(storeQueryServerConfig{Address: "127.0.0.1:0", Concurrency: 5}, nil)
	require.Equal(t, 5, cap(srv.sema))
	require.Equal(t, 13, srv.workerLimit, "the worker pool tracks the admission slots")
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
