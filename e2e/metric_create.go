package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// This file pre-creates the value_p metrics via POST /api/metric.
//
// value_p NEVER auto-creates (internal/data_model/autocreate.go derives only
// counter/value/unique from the wire payload), so the harness must create the
// mapping BEFORE the client writes — otherwise every value_p packet maps to a
// non-percentile metric and the percentile queries return nothing. The POST
// happens on the HOST against the api's published address (the same address the
// assertions query); --local-mode grants admin+developer with no auth token
// (internal/api/access.go), so no Authorization header is needed.
//
// POST /api/metric body is a MetricInfo{Metric: format.MetricMetaValue}:
// {"metric":{"name":"<name>","kind":"value_p","tags":[{"name":"0"},…]}}. The tags
// array maps the group-by positions up front (see createValuePMetrics). metric_id
// omitted → create (internal/api/handler.go: handlePostMetric,
// create := metric.MetricID == 0). POSTing a name that already exists is NOT a
// clean re-create — but it never arises: every metric name embeds the unique
// runID, so each POST targets a fresh name.

// createValuePMetrics POSTs every value_p metric in the stream. It is best-effort
// per metric: a failure is returned so the caller fails the phase rather than
// letting the driver write into un-mapped metrics. A 200 is success; anything
// else fails the phase for that metric.
func createValuePMetrics(ctx context.Context, rec *recorder, apiAddr string, stream metricStream) error {
	for _, m := range stream.Metrics {
		if m.Kind != kindValueP {
			continue
		}
		// Include an explicit tag mapping for every group-by key (m.QBKeys) so the
		// tag POSITION is mapped at creation, not lazily through the slow
		// synchronizeWithJournal path (autocreate.go) that a pre-created metric
		// otherwise relies on to add unmapped tag positions. A mapped position lets
		// MapValidateTag resolve the tag VALUE→ID (with the string fallback) on the
		// very first write, so the series split AND the reverse string resolution at
		// query time both work — matching auto-created counter metrics, whose tag
		// values render correctly in series_meta. Without this, value_p series land
		// (the data is correct) but their tags resolve to empty in series_meta and
		// the asserter cannot match them. The tag NAME is the positional index
		// ("0".."47"), exactly what the harness's positional writes carry.
		tagObjs := make([]string, 0, len(m.QBKeys))
		for _, k := range m.QBKeys {
			tagObjs = append(tagObjs, fmt.Sprintf(`{"name":%q}`, k))
		}
		body := fmt.Sprintf(`{"metric":{"name":%q,"kind":"value_p","tags":[%s]}}`,
			m.Name, strings.Join(tagObjs, ","))
		url := "http://" + apiAddr + "/api/metric?s=" + m.Name
		resp, code, err := httpPostJSON(ctx, url, body)
		if err != nil {
			return fmt.Errorf("create value_p metric %s: POST %s: %w", m.Name, url, err)
		}
		if code != http.StatusOK {
			return fmt.Errorf("create value_p metric %s: HTTP %d: %s", m.Name, code, truncate(resp, 300))
		}
		rec.logf("created value_p metric %s (POST /api/metric → 200)", m.Name)
	}
	return nil
}

// httpPostJSON issues a JSON POST with a bounded timeout and returns the body,
// status code, and error. Mirrors httpGet's shape so callers handle both alike.
func httpPostJSON(ctx context.Context, url, body string) (string, int, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, rerr := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, rerr
}
