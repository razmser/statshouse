// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useEffect, useMemo, useState } from 'react';
import produce from 'immer';
import cn from 'classnames';
import * as utils from '../../view/utils';
import { getTimeShifts, timeShiftAbbrevExpand } from '../../view/utils';
import { MetricItem } from '../../hooks';
import { PlotControlFrom, PlotControlTimeShifts, PlotControlTo } from '../index';
import {
  selectorParamsTimeShifts,
  selectorPlotsDataByIndex,
  selectorSetParams,
  selectorSetTimeRange,
  selectorTimeRange,
  useStore,
} from '../../store';
import { metricKindToWhat, metricMeta, querySelector, queryWhat } from '../../view/api';
import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { ReactComponent as SVGFilter } from 'bootstrap-icons/icons/filter.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { globalSettings } from '../../common/settings';

export const PlotControlsPromQL = memo(function PlotControlsPromQL_(props: {
  indexPlot: number;
  setBaseRange: (r: utils.timeRangeAbbrev) => void;
  sel: querySelector;
  setSel: (state: React.SetStateAction<querySelector>, replaceUrl?: boolean) => void;
  meta: metricMeta;
  numQueries: number;
  metricsOptions: MetricItem[];
  clonePlot?: () => void;
}) {
  const { indexPlot, setBaseRange, sel, setSel, meta } = props;
  const [promQL, setPromQL] = useState(sel.promQL);

  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plotData = useStore(selectorPlotsData);

  const timeShifts = useStore(selectorParamsTimeShifts);
  const setParams = useStore(selectorSetParams);

  const timeRange = useStore(selectorTimeRange);
  const setTimeRange = useStore(selectorSetTimeRange);

  // keep meta up-to-date when sel.metricName changes (e.g. because of navigation)
  useEffect(() => {
    const whats = metricKindToWhat(meta.kind);
    if (meta.name === sel.metricName && sel.what.some((qw) => whats.indexOf(qw) === -1)) {
      // console.log('reset what', meta, sel.metricName, sel.what, whats);
      setSel(
        (s) => ({
          ...s,
          what: [whats[0]],
        }),
        true
      );
    }
  }, [meta.kind, meta.name, sel.metricName, sel.what, setSel]);

  const onCustomAggChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      const customAgg = parseInt(e.target.value);
      const timeShiftsSet = getTimeShifts(customAgg);
      const shifts = timeShifts.filter(
        (v) => timeShiftsSet.find((shift) => timeShiftAbbrevExpand(shift) === v) !== undefined
      );
      setParams((p) => ({ ...p, timeShifts: shifts }));
      setSel((s) => ({
        ...s,
        customAgg: customAgg,
      }));
    },
    [setParams, setSel, timeShifts]
  );

  const onHostChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      setSel((s) => ({
        ...s,
        maxHost: e.target.checked,
      }));
    },
    [setSel]
  );

  const inputPromQL = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      const value = e.currentTarget.value;
      setPromQL(value);
    },
    [setPromQL]
  );

  const toFilter = useCallback(() => {
    setSel(
      produce((s) => {
        if (plotData.nameMetric) {
          s.metricName = plotData.nameMetric;
          s.what = (
            plotData.whats?.length ? plotData.whats.slice() : globalSettings.default_metric_what.slice()
          ) as queryWhat[];
          s.customName = '';
          s.groupBy = [];
          s.filterIn = {};
          s.filterNotIn = {};
          s.promQL = '';
        } else {
          s.metricName = globalSettings.default_metric;
          s.what = globalSettings.default_metric_what.slice();
          s.customName = '';
          s.groupBy = globalSettings.default_metric_group_by.slice();
          s.filterIn = { ...globalSettings.default_metric_filter_in };
          s.filterNotIn = { ...globalSettings.default_metric_filter_not_in };
          s.promQL = '';
        }
      })
    );
  }, [plotData.nameMetric, plotData.whats, setSel]);

  const sendPromQL = useCallback(() => {
    setSel(
      produce((p) => {
        p.promQL = promQL;
      })
    );
  }, [promQL, setSel]);

  const resetPromQL = useCallback(() => {
    setPromQL(sel.promQL);
  }, [sel.promQL]);

  useEffect(() => {
    setPromQL(sel.promQL);
  }, [sel.promQL, setPromQL]);

  return (
    <div>
      <div>
        <div className="row mb-3 align-items-baseline">
          <div className="col-12 d-flex align-items-baseline">
            <select
              className={cn('form-select me-4', sel.customAgg > 0 && 'border-warning')}
              value={sel.customAgg}
              onChange={onCustomAggChange}
            >
              <option value={0}>Auto</option>
              <option value={-1}>Auto (low)</option>
              <option value={1}>1 second</option>
              <option value={5}>5 seconds</option>
              <option value={15}>15 seconds</option>
              <option value={60}>1 minute</option>
              <option value={5 * 60}>5 minutes</option>
              <option value={15 * 60}>15 minutes</option>
              <option value={60 * 60}>1 hour</option>
              <option value={4 * 60 * 60}>4 hours</option>
              <option value={24 * 60 * 60}>24 hours</option>
              <option value={7 * 24 * 60 * 60}>7 days</option>
              <option value={31 * 24 * 60 * 60}>1 month</option>
            </select>
            <div className="form-check form-switch">
              <input
                className="form-check-input"
                type="checkbox"
                value=""
                id="switchMaxHost"
                checked={sel.maxHost}
                onChange={onHostChange}
              />
              <label className="form-check-label" htmlFor="switchMaxHost" title="Host">
                <SVGPcDisplay />
              </label>
            </div>
            <button type="button" className="btn btn-outline-primary ms-3" title="Filter" onClick={toFilter}>
              <SVGFilter />
            </button>
          </div>
        </div>
        <div className="row mb-3 align-items-baseline">
          <PlotControlFrom timeRange={timeRange} setTimeRange={setTimeRange} setBaseRange={setBaseRange} />
          <div className="align-items-baseline mt-2">
            <PlotControlTo timeRange={timeRange} setTimeRange={setTimeRange} />
          </div>
          <PlotControlTimeShifts className="w-100 mt-2" />
        </div>

        <div className="row mb-3 align-items-baseline">
          <div className="input-group">
            <textarea className="form-control font-monospace" rows={8} value={promQL} onInput={inputPromQL}></textarea>
          </div>
          <div className="d-flex flex-row justify-content-end mt-2">
            <button type="button" className="btn btn-outline-primary me-2" title="Reset PromQL" onClick={resetPromQL}>
              <SVGArrowCounterclockwise />
            </button>
            <span className="flex-grow-1"></span>
            <button type="button" className="btn btn-outline-primary" onClick={sendPromQL}>
              Run
            </button>
          </div>
        </div>
      </div>
    </div>
  );
});
