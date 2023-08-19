// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo, useState } from 'react';
import cn from 'classnames';
import { TagSelect } from '../TagSelect';
import { SwitchBox } from '../UI';
import { formatTagValue } from '../../view/api';

import { ReactComponent as SVGLayers } from 'bootstrap-icons/icons/layers.svg';
import { SelectOptionProps } from '../Select';
import { formatPercent, normalizeTagValues } from '../../view/utils';
import { MetricMetaTag } from '../../api/metric';
import { MetricTagValueInfo } from '../../api/metricTagValues';
import { escapeHTML } from '../../common/helpers';

const emptyListArray: MetricTagValueInfo[] = [];

export type VariableControlProps<T> = {
  target?: T;
  placeholder?: string;
  negative?: boolean;
  setNegative: (name: T | undefined, value: boolean) => void;
  groupBy?: boolean;
  setGroupBy: (name: T | undefined, value: boolean) => void;
  className?: string;
  values: string[];
  onChange: (name: T | undefined, value: string[]) => void;
  tagMeta?: MetricMetaTag;
  more?: boolean;
  loaded?: boolean;
  list?: MetricTagValueInfo[];
  small?: boolean;
  setOpen?: (name: T | undefined, value: boolean) => void;
  customBadge?: React.ReactNode;
};
export function VariableControl<T>({
  target,
  placeholder,
  className,
  negative = false,
  setNegative,
  groupBy = false,
  setGroupBy,
  values,
  onChange,
  list = emptyListArray,
  loaded,
  more,
  tagMeta,
  small,
  setOpen,
  customBadge,
}: VariableControlProps<T>) {
  const [sortByName, setSortByName] = useState(false);

  const listSort = useMemo<SelectOptionProps[]>(
    () =>
      normalizeTagValues(list, !sortByName).map((v) => {
        const name = formatTagValue(v.value, tagMeta?.value_comments?.[v.value], tagMeta?.raw, tagMeta?.raw_kind);
        const percent = formatPercent(v.count);
        const title = tagMeta?.value_comments?.[v.value]
          ? `${name} (${formatTagValue(v.value, undefined, tagMeta?.raw, tagMeta?.raw_kind)}): ${percent}`
          : `${name}: ${percent}`;

        return {
          name: name,
          html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${escapeHTML(
            name
          )}</div><div class="text-end">${escapeHTML(percent)}</div></div>`,
          value: v.value,
          title: title,
        };
      }),
    [list, sortByName, tagMeta?.raw, tagMeta?.raw_kind, tagMeta?.value_comments]
  );

  const onSelectFocus = useCallback(() => {
    setOpen?.(target, true);
  }, [setOpen, target]);

  const onSelectBlur = useCallback(() => {
    setOpen?.(target, false);
  }, [setOpen, target]);

  const onChangeFilter = useCallback(
    (value?: string | string[] | undefined) => {
      const v = value == null ? [] : Array.isArray(value) ? value : [value];
      onChange(target, v);
    },
    [target, onChange]
  );
  const onSetNegative = useCallback(
    (value: boolean) => {
      setNegative(target, value);
    },
    [target, setNegative]
  );
  const onSetGroupBy = useCallback(
    (value: boolean) => {
      setGroupBy(target, value);
    },
    [target, setGroupBy]
  );
  const onRemoveFilter = useCallback<React.MouseEventHandler<HTMLButtonElement>>(
    (event) => {
      const value = event.currentTarget.getAttribute('data-value');
      onChange(
        target,
        values.filter((v) => v !== value)
      );
    },
    [target, onChange, values]
  );
  return (
    <div className={className}>
      <div className="d-flex align-items-center">
        <div className={cn('input-group flex-nowrap w-100', small ? 'input-group-sm me-2' : 'input-group  me-4')}>
          <TagSelect
            values={values}
            placeholder={placeholder}
            loading={loaded}
            onChange={onChangeFilter}
            moreOption={more}
            options={listSort}
            onFocus={onSelectFocus}
            onBlur={onSelectBlur}
            negative={negative}
            setNegative={onSetNegative}
            sort={sortByName}
            setSort={setSortByName}
          />
        </div>
        <SwitchBox title="Group by" checked={groupBy} onChange={onSetGroupBy}>
          <SVGLayers />
        </SwitchBox>
      </div>
      <div className="d-flex flex-wrap">
        {customBadge}
        {values?.map((v) => (
          <button
            type="button"
            key={v}
            data-value={v}
            className={cn(
              'overflow-force-wrap btn btn-sm pt-0 pb-0 mt-2 me-2',
              negative ? 'btn-danger' : 'btn-success'
            )}
            style={{ userSelect: 'text' }}
            onClick={onRemoveFilter}
          >
            {formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
          </button>
        ))}
      </div>
    </div>
  );
}