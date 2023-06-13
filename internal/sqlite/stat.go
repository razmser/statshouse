package sqlite

import (
	"time"

	"github.com/vkcom/statshouse-go"
)

type StatsOptions struct {
	Service string
	Cluster string
	DC      string
}

const (
	queryDurationMetric  = "sqlite_query_duration"
	waitDurationMetric   = "sqlite_wait_duration"
	txDurationMetric     = "sqlite_tx_duration"
	actionDurationMetric = "sqlite_action_duration"
	applyQueueSizeMetric = "sqlite_apply_queue_size"

	txDo   = "sqlite_tx_do"
	txView = "sqlite_tx_view"

	waitView       = "wait_lock_view"
	waitDo         = "wait_lock_do"
	waitBinlogSync = "binlog_wait_db_sync"

	query = "query"
	exec  = "exec"
)

func (s *StatsOptions) checkEmpty() bool {
	return s.Service == ""
}

func (s *StatsOptions) measureSqliteQueryDurationSince(typ, name string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(queryDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ, 5: name}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureWaitDurationSince(typ string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(waitDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureActionDurationSince(typ string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(actionDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureSqliteTxDurationSince(typ, name string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(txDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ, 5: name}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) applyQueueSize(registry *statshouse.Client, size int64) {
	if s.checkEmpty() {
		return
	}
	registry.Metric(applyQueueSizeMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC}).Value(float64(size))
}
