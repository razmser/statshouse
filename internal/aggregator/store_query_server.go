// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/VKCOM/tl/pkg/rpc"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"github.com/VKCOM/statshouse/internal/vkgo/build"
)

// Query-traffic defaults, sized for the smallest viable node. The ingest
// listener is configured with unlimited workers, disabled context timeouts and
// no default response timeout — settings that must not leak onto query
// traffic, which is why queries get their own listener with their own numbers.
const (
	// DefaultQueryConcurrency is how many store queries may execute at once
	// per shard; the next one is refused as overloaded rather than queued.
	DefaultQueryConcurrency = 2

	// DefaultQueryTimeout bounds a store query that did not ask for a
	// timeout. It is applied both as the body timeout (when timeout_ms is
	// absent) and as the listener's default response timeout.
	DefaultQueryTimeout = 60 * time.Second

	// MaxQueryTimeout is the ceiling timeout_ms is clamped to, so a
	// misbehaving client cannot pin query slots for hours.
	MaxQueryTimeout = 5 * time.Minute
)

// storeQueryExecutor runs the two structured store-query verbs against the
// shard's store. The DuckDB renderers (series and tag values) implement it
// behind the aggregator's journal validation. The executor must honour its
// context: a cancelled context must actually stop the underlying query
// (duckdb-go interrupts DuckDB), because admission slots are only released
// once the call returns.
type storeQueryExecutor interface {
	QuerySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error)
	QueryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error)
}

// storeQueryJournalWait bounds how long a store query waits for the local
// journal to reach the request's metric_version before deciding the two
// journals disagree. It is a variable only so tests can shrink it; production
// leaves it alone.
var storeQueryJournalWait = 5 * time.Second

// validateStoreQueryMetadata checks a store query against the aggregator's
// own journal before any row is read: every metric the query addresses must
// exist there — unknown_metric otherwise — and the request's tag layout must
// equal the journal's own derivation — metadata_mismatch otherwise. A layout
// the journal disagrees with would reinterpret stored bytes (a tag flipping
// between mapped and raw64 changes what tagN means), so the query is refused
// instead; the API retries with a fresh layout.
//
// The check first waits briefly for the journal to reach the request's
// metric_version, so a query racing journal propagation between the API and
// this shard converges rather than failing — the request's version exists
// exactly so the two sides can meet.
func validateStoreQueryMetadata(ctx context.Context, storage *metajournal.MetricsStorage, base tlstatshouse.StoreQueryBase) error {
	ids := addressedMetricIDs(base)
	if len(ids) == 0 {
		// nothing specific is addressed; the layout is the API's own choice
		// and there is no journal entry to check it against
		return nil
	}
	if base.MetricVersion > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, storeQueryJournalWait)
		err := storage.WaitVersion(waitCtx, base.MetricVersion)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // the query itself died; the caller maps it
			}
			return duckstore.NewError(duckstore.ErrCodeMetadataMismatch,
				"journal has not reached version %d: the tag layout cannot be validated", base.MetricVersion)
		}
	}
	for _, id := range ids {
		if builtin, ok := format.BuiltinMetrics[id]; ok {
			// builtin metrics never live in the journal — every process
			// carries their metadata compiled in, and their rows land in the
			// store through the same write path as user metrics'. Their tag
			// layout is fixed at build time, so the registry is the thing to
			// validate against; a disagreement is the same refusal as a user
			// metric's (an API binary built from different sources).
			if !duckstore.TagLayoutsEqual(duckstore.TagLayoutKinds(builtin), base.TagLayout.Kinds) {
				return duckstore.NewError(duckstore.ErrCodeMetadataMismatch,
					"tag layout of builtin metric %d disagrees with the registry: refusing to reinterpret rows", id)
			}
			continue
		}
		metric := storage.GetMetaMetric(id)
		if metric == nil {
			return duckstore.NewError(duckstore.ErrCodeUnknownMetric,
				"metric %d is absent from the journal", id)
		}
		if !duckstore.TagLayoutsEqual(duckstore.TagLayoutKinds(metric), base.TagLayout.Kinds) {
			return duckstore.NewError(duckstore.ErrCodeMetadataMismatch,
				"tag layout of metric %d disagrees with the journal at version %d: refusing to reinterpret rows",
				id, base.MetricVersion)
		}
	}
	return nil
}

// addressedMetricIDs returns the metric ids a store query reads: its explicit
// metric_id, else the members of its metric_in list. A query that only
// excludes metrics (metric_not_in alone) addresses none in particular.
func addressedMetricIDs(base tlstatshouse.StoreQueryBase) []int32 {
	if base.MetricId != 0 {
		return []int32{base.MetricId}
	}
	var ids []int32
	if base.IsSetMetricIn() {
		for _, id := range base.MetricIn {
			if id != 0 {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

// storeQueryServerConfig configures the query listener.
type storeQueryServerConfig struct {
	// Address the listener serves store queries on ("host:port").
	Address string

	// Concurrency is how many queries execute at once; one more than that
	// is refused as overloaded. Defaults to DefaultQueryConcurrency.
	Concurrency int

	// CryptoKeys and TrustedSubnetGroups configure the RPC transport the same
	// way the ingest listener's are configured.
	CryptoKeys          []string
	TrustedSubnetGroups [][]string

	// Logf receives operator-facing messages. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Cancellation causes. A query's context is created with WithCancelCause so
// the executor — and the error mapping — can tell a dropped client from an
// elapsed deadline apart, whichever of the two fired first.
var (
	errStoreQueryCanceledByClient = errors.New("store query canceled: client dropped the RPC")
	errStoreQueryTransportTimeout = errors.New("store query canceled: response deadline elapsed")
)

// storeQueryServer is the aggregator's second RPC listener: the one query
// traffic arrives on. It deliberately shares none of the ingest listener's
// permissive settings — workers are bounded, request timeouts apply to the
// handler context, and the default response timeout is nonzero — and it holds
// the admission control that keeps a burst of heavy queries from eating the
// machine ingestion needs.
//
// Each admitted query is registered as an RPC longpoll, which is the
// transport's cancellation registry: a client that cancels the call or drops
// the connection takes the query's context with it, so the executor (and
// through QueryContext, DuckDB itself) actually stops instead of burning a
// slot to completion.
type storeQueryServer struct {
	cfg      storeQueryServerConfig
	executor storeQueryExecutor
	sema     chan struct{} // admission slots; capacity Concurrency, non-blocking acquire
	h        tlstatshouse.Handler
	server   *rpc.Server

	// The two transport settings that most visibly differ from the ingest
	// listener's, recorded for the settings test.
	workerLimit            int
	defaultResponseTimeout time.Duration
}

func newStoreQueryServer(cfg storeQueryServerConfig, executor storeQueryExecutor) *storeQueryServer {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultQueryConcurrency
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	s := &storeQueryServer{
		cfg:                    cfg,
		executor:               executor,
		sema:                   make(chan struct{}, cfg.Concurrency),
		workerLimit:            cfg.Concurrency + 8,
		defaultResponseTimeout: DefaultQueryTimeout,
	}
	s.h = tlstatshouse.Handler{
		RawStoreQuerySeries:    s.handleQuerySeries,
		RawStoreQueryTagValues: s.handleQueryTagValues,
	}
	// Queries are dispatched from the sync handler: it is the only place the
	// transport lets a longpoll start, because the sync path is also the path
	// RpcCancelReq takes, which is what makes cancelling an in-flight query
	// possible at all. Handlers there run on the connection reader, so they
	// must return quickly — which is exactly what admission-plus-own-goroutine
	// guarantees: a query never blocks the connection reading its own cancel.
	//
	// Not the ingest server's options: workers are bounded (they only serve
	// functions this handler does not know), and a request without an explicit
	// timeout gets one.
	s.server = rpc.NewServer(rpc.ServerWithCryptoKeys(cfg.CryptoKeys),
		rpc.ServerWithLogf(cfg.Logf),
		rpc.ServerWithMaxWorkers(s.workerLimit),
		rpc.ServerWithSyncHandler(s.handleQueryClient),
		rpc.ServerWithTrustedSubnetGroups(cfg.TrustedSubnetGroups),
		rpc.ServerWithVersion(build.Info()),
		rpc.ServerWithDefaultResponseTimeout(s.defaultResponseTimeout),
		rpc.ServerWithResponseBufSize(1<<20),
		rpc.ServerWithResponseMemEstimate(1<<20),
		rpc.ServerWithResponseMemoryLimit(1<<30),
	)
	return s
}

// handleQueryClient is the sync dispatch: the two store-query verbs go to the
// generated handler, anything else falls through to the transport's ordinary
// handling.
func (s *storeQueryServer) handleQueryClient(ctx context.Context, hctx *rpc.HandlerContext) error {
	return s.h.Handle(ctx, hctx)
}

// ListenAndServe starts serving on the configured address; it returns when the
// server is shut down.
func (s *storeQueryServer) ListenAndServe() error {
	return s.server.ListenAndServe("tcp4", s.cfg.Address)
}

// Serve serves on an already-open listener; tests use it to pick a free port.
func (s *storeQueryServer) Serve(ln net.Listener) error {
	return s.server.Serve(ln)
}

// Shutdown stops accepting queries and cancels the ones in flight (closing the
// connections takes their longpolls — and thus their contexts — with it).
func (s *storeQueryServer) Shutdown() {
	s.server.Shutdown()
}

// CloseWait waits for the listener's connections to drain.
func (s *storeQueryServer) CloseWait(ctx context.Context) error {
	return s.server.CloseWait(ctx)
}

// runningStoreQuery is one admitted query: its longpoll handle, the cause-
// carrying cancel for its execution context, and the transport deadline the
// longpoll tree fires at. It implements rpc.LongpollCanceller, which is how
// the transport tells us the client is gone.
type runningStoreQuery struct {
	cancel   context.CancelCauseFunc
	deadline time.Time // when the transport gives up on the response; zero = never
}

// CancelLongpoll is called on an explicit RpcCancelReq from the client.
func (q *runningStoreQuery) CancelLongpoll(lh rpc.LongpollHandle) {
	q.cancel(errStoreQueryCanceledByClient)
}

// WriteEmptyResponse is called when the connection drops or the response
// deadline elapses. The deadline case must classify as a timeout, not a
// cancellation, so a client that asked for too much sees deadline_exceeded
// rather than a lie about itself.
func (q *runningStoreQuery) WriteEmptyResponse(lh rpc.LongpollHandle, hctx *rpc.HandlerContext) error {
	if !q.deadline.IsZero() && !time.Now().Before(q.deadline) {
		q.cancel(errStoreQueryTransportTimeout)
	} else {
		q.cancel(errStoreQueryCanceledByClient)
	}
	return rpc.ErrLongpollNoEmptyResponse
}

// handleQuerySeries and handleQueryTagValues share one body: parse, admit,
// clamp the timeout, register for cancellation, then run the executor on its
// own goroutine and answer through the longpoll. Nothing here takes a lock on
// the ingest path, and admission is refused before any work starts, so
// ingestion never yields to queries.
func (s *storeQueryServer) handleQuerySeries(ctx context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.StoreQuerySeries
	if _, err := args.ReadTL1(hctx.Request); err != nil {
		return badStoreQueryRequest("storeQuerySeries", err)
	}
	return executeQuery(s, ctx, hctx, args.Base,
		func(qctx context.Context) (tlstatshouse.StoreSeriesResponse, error) {
			return s.executor.QuerySeries(qctx, args)
		},
		func(respCtx *rpc.HandlerContext, resp tlstatshouse.StoreSeriesResponse) error {
			var err error
			respCtx.Response, err = args.WriteResultTL1(respCtx.Response, resp)
			return err
		})
}

func (s *storeQueryServer) handleQueryTagValues(ctx context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.StoreQueryTagValues
	if _, err := args.ReadTL1(hctx.Request); err != nil {
		return badStoreQueryRequest("storeQueryTagValues", err)
	}
	return executeQuery(s, ctx, hctx, args.Base,
		func(qctx context.Context) (tlstatshouse.StoreTagValuesResponse, error) {
			return s.executor.QueryTagValues(qctx, args)
		},
		func(respCtx *rpc.HandlerContext, resp tlstatshouse.StoreTagValuesResponse) error {
			var err error
			respCtx.Response, err = args.WriteResultTL1(respCtx.Response, resp)
			return err
		})
}

func badStoreQueryRequest(verb string, err error) error {
	return duckstore.NewError(duckstore.ErrCodeBadRequest, "malformed %s request: %v", verb, err)
}

// executeQuery runs one admitted store query. It returns nil when the answer
// (or the error) is sent through the longpoll; a non-nil return travels the
// ordinary response path, which is what refusals use. run executes the verb
// with the query's context; write serializes its result into the response
// context — which FinishLongpoll hands out only afterwards, a fresh context
// rather than the handler's own (the transport reuses that one the moment the
// handler returns).
func executeQuery[R any](s *storeQueryServer, ctx context.Context, hctx *rpc.HandlerContext, base tlstatshouse.StoreQueryBase,
	run func(qctx context.Context) (R, error), write func(respCtx *rpc.HandlerContext, resp R) error) error {
	// Admission before any work: the slot is refused, not queued. It is held
	// for as long as the query executes and released only after its answer is
	// built, so the semaphore counts executing queries, not pending handlers.
	select {
	case s.sema <- struct{}{}:
	default:
		return duckstore.NewError(duckstore.ErrCodeOverloaded,
			"all %d query slots busy, retry later", cap(s.sema))
	}

	// Two cancel layers: the cause-carrying cancel lets the longpoll
	// canceller record WHY the query died, and the timeout layer bounds it by
	// the request's clamped timeout_ms (its expiry carries the transport
	// deadline cause, so an elapsed timeout classifies as one).
	timeout := clampQueryTimeout(base.TimeoutMs)
	qctx, cancel := context.WithCancelCause(ctx)
	qctx, timeoutCancel := context.WithTimeoutCause(qctx, timeout, errStoreQueryTransportTimeout)

	q := &runningStoreQuery{cancel: cancel, deadline: longpollDeadline(hctx)}
	lh, err := hctx.StartLongpoll(q)
	if err != nil {
		timeoutCancel()
		cancel(nil)
		<-s.sema
		return err // server shutting down
	}
	// The query runs on its own goroutine: the sync handler (and the
	// connection reader behind it) is freed immediately, so a running query
	// never blocks the connection reading its own cancel, and heavy queries
	// cannot block the listener's other traffic.
	go func() {
		defer timeoutCancel()
		defer func() { cancel(nil) }() // plain cleanup once the query is done
		defer func() { <-s.sema }()
		resp, err := run(qctx)
		respCtx, ok := lh.FinishLongpoll()
		if !ok {
			// The client already cancelled or dropped: nothing to send, the
			// context cancellation above already stopped the query.
			return
		}
		if err == nil {
			err = write(respCtx, resp)
		}
		if err != nil {
			err = mapStoreQueryError(qctx, err)
		}
		respCtx.SendLongpollResponse(err)
	}()
	return nil
}

// clampQueryTimeout interprets the request's timeout_ms: absent or zero means
// the default, and anything above the ceiling is cut to it.
func clampQueryTimeout(timeoutMs int32) time.Duration {
	if timeoutMs <= 0 {
		return DefaultQueryTimeout
	}
	d := time.Duration(timeoutMs) * time.Millisecond
	if d > MaxQueryTimeout {
		return MaxQueryTimeout
	}
	return d
}

// longpollDeadline mirrors the transport's own arithmetic for when the
// longpoll tree fires the empty response (requestTime + timeout*7/8), so
// WriteEmptyResponse can classify a fire as a deadline rather than a drop.
// The client's custom timeout wins over the listener default, exactly as in
// the transport's fillInvokeReqInternals.
func longpollDeadline(hctx *rpc.HandlerContext) time.Time {
	timeout := DefaultQueryTimeout
	if custom := time.Duration(hctx.RequestExtra.CustomTimeoutMs) * time.Millisecond; custom > 0 {
		timeout = custom
	}
	return hctx.RequestTime().Add(timeout * 7 / 8)
}

// mapStoreQueryError maps an executor failure onto the structured store error
// codes. Errors that already carry a code (row_limit from the renderer, an
// overloaded refusal, ...) pass through untouched; context failures classify
// by their cause; everything else becomes internal so a raw error never leaks
// to a client as an unstructured rpc failure.
func mapStoreQueryError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var rpcErr *rpc.Error
	if errors.As(err, &rpcErr) {
		return err
	}
	cause := context.Cause(ctx)
	switch {
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(cause, context.DeadlineExceeded),
		errors.Is(cause, errStoreQueryTransportTimeout):
		return duckstore.NewError(duckstore.ErrCodeDeadlineExceeded, "query timed out (%v)", err)
	case errors.Is(err, context.Canceled),
		errors.Is(cause, context.Canceled),
		errors.Is(cause, errStoreQueryCanceledByClient):
		return duckstore.NewError(duckstore.ErrCodeCanceled, "client dropped the query (%v)", err)
	default:
		return duckstore.NewError(duckstore.ErrCodeInternal, "%v", err)
	}
}
