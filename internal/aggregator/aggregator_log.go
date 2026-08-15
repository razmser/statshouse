// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
)

func (a *Aggregator) appendInternalLogLocked(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string) {
	nowUnix := uint32(time.Now().Unix())
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[0:], nowUnix)

	a.internalLog = append(a.internalLog, tmp[:]...)
	a.internalLog = rowbinary.AppendString(a.internalLog, srvfunc.Hostname())
	a.internalLog = rowbinary.AppendString(a.internalLog, typ)
	a.internalLog = rowbinary.AppendString(a.internalLog, key0)
	a.internalLog = rowbinary.AppendString(a.internalLog, key1)
	a.internalLog = rowbinary.AppendString(a.internalLog, key2)
	a.internalLog = rowbinary.AppendString(a.internalLog, key3)
	a.internalLog = rowbinary.AppendString(a.internalLog, key4)
	a.internalLog = rowbinary.AppendString(a.internalLog, key5)
	a.internalLog = rowbinary.AppendString(a.internalLog, message)
}

func (a *Aggregator) appendInternalLog(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appendInternalLogLocked(typ, key0, key1, key2, key3, key4, key5, message)
}

// internalLogRow is one decoded row of the aggregator's internal log: the
// columns of the statshouse_internal_log_buffer table the ClickHouse backend
// inserts into. Under duck-store the same rows go to the process log instead,
// so they have to come back out of the RowBinary buffer first.
type internalLogRow struct {
	Time    uint32
	Host    string
	Type    string
	Keys    [6]string
	Message string
}

// decodeInternalLogRows decodes the RowBinary internal-log buffer that
// appendInternalLogLocked builds (uint32 time, then nine length-prefixed
// strings), handing every row to emit. The encoding matches
// rowbinary.AppendString, which prefixes each string with its uvarint length.
func decodeInternalLogRows(buf []byte, emit func(internalLogRow)) error {
	for len(buf) > 0 {
		var row internalLogRow
		if len(buf) < 4 {
			return fmt.Errorf("internal log truncated: %d trailing bytes", len(buf))
		}
		row.Time = binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]
		var err error
		if buf, err = readInternalLogString(buf, &row.Host); err != nil {
			return err
		}
		if buf, err = readInternalLogString(buf, &row.Type); err != nil {
			return err
		}
		for i := range row.Keys {
			if buf, err = readInternalLogString(buf, &row.Keys[i]); err != nil {
				return err
			}
		}
		if buf, err = readInternalLogString(buf, &row.Message); err != nil {
			return err
		}
		emit(row)
	}
	return nil
}

func readInternalLogString(buf []byte, s *string) ([]byte, error) {
	n, adv := binary.Uvarint(buf)
	if adv <= 0 {
		return buf, fmt.Errorf("internal log has an invalid string length prefix")
	}
	if uint64(len(buf)-adv) < n {
		return buf, fmt.Errorf("internal log string truncated: want %d bytes, have %d", n, len(buf)-adv)
	}
	*s = string(buf[adv : uint64(adv)+n])
	return buf[uint64(adv)+n:], nil
}

// We do not want to wait this func to finish, so no attempts to cancel
func (a *Aggregator) goInternalLog() {
	httpClient := makeHTTPClient()
	var localLog []byte
	for {
		time.Sleep(data_model.InternalLogInsertInterval)

		a.mu.Lock()
		if len(a.internalLog) != 0 {
			localLog, a.internalLog = a.internalLog, localLog
		}
		a.mu.Unlock()

		if len(localLog) != 0 {
			if a.config.StorageBackend == duckstore.BackendDuck {
				// duck-store has no ClickHouse log-buffer table to insert
				// into (and nothing queries one via the API), so the internal
				// log — including the insert-error log — goes to the process
				// log instead of being silently dropped.
				if err := decodeInternalLogRows(localLog, func(row internalLogRow) {
					log.Printf("[internal_log] %s host=%s type=%s keys=[%s %s %s %s %s %s] message=%s",
						time.Unix(int64(row.Time), 0).UTC().Format("2006-01-02 15:04:05"), row.Host, row.Type,
						row.Keys[0], row.Keys[1], row.Keys[2], row.Keys[3], row.Keys[4], row.Keys[5], row.Message)
				}); err != nil {
					log.Printf("error decoding internal log - %v", err)
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), data_model.ClickHouseTimeoutInsert)
				status, exception, _, err := sendToClickhouse(ctx, httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, "statshouse_internal_log_buffer(time,host,type,key0,key1,key2,key3,key4,key5,message)", localLog, "")
				cancel()
				if err != nil {
					a.appendInternalLog("insert_error", "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_internal_log_buffer", "", "", err.Error()) // Hopefully will insert next time
					log.Printf("error inserting internal log - %v", err)
				}
			}
			localLog = localLog[:0] // Will be swapped on the next iteration
		}
	}
}

func (a *Aggregator) reportInsertMetric(t uint32, metricInfo *format.MetricMetaValue, historic bool, err error, status int, exception int, inflightType int32, value float64) {
	historicTag := int32(format.TagValueIDConveyorRecent)
	if historic {
		historicTag = format.TagValueIDConveyorHistoric
	}
	statusTag := int32(format.TagValueIDStatusOK)
	if err != nil {
		statusTag = format.TagValueIDStatusError
	}
	a.sh2.AddValueCounter(t, metricInfo,
		[]int32{0, 0, 0, 0, historicTag, statusTag, int32(status), int32(exception), format.TagValueIDAggInsertV3, inflightType},
		value, 1)
}
