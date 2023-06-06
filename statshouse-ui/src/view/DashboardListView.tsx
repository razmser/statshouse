// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DashboardListStore, useDashboardListStore } from '../store';
import React, { useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { useStateInput } from '../hooks';
import { mapKeyboardEnToRu, mapKeyboardRuToEn, toggleKeyboard } from '../common/toggleKeyboard';
import { ErrorMessages } from '../components';

export type DashboardListViewProps = {};

const { update } = useDashboardListStore.getState();
const selectorDashboardList = ({ list }: DashboardListStore) => list;

export const DashboardListView: React.FC<DashboardListViewProps> = () => {
  const list = useDashboardListStore(selectorDashboardList);
  const searchInput = useStateInput('');
  useEffect(() => {
    update();
  }, []);

  const filterList = useMemo(() => {
    const orig = searchInput.value.toLocaleLowerCase();
    const ru = toggleKeyboard(orig, mapKeyboardEnToRu);
    const en = toggleKeyboard(orig, mapKeyboardRuToEn);
    const res = list.filter(
      (item) =>
        searchInput.value === '' ||
        item.name.toLowerCase().includes(orig) ||
        item.description.toLowerCase().includes(orig) ||
        item.name.toLowerCase().includes(ru) ||
        item.description.toLowerCase().includes(ru) ||
        item.name.toLowerCase().includes(en) ||
        item.description.toLowerCase().includes(en)
    );
    res.sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 0
    );
    return res;
  }, [list, searchInput.value]);

  return (
    <div className="container-sm pt-3 pb-3 w-max-720">
      <div className="mb-2">
        <input
          id="dashboard-list-search"
          type="search"
          placeholder="Search"
          className="form-control"
          aria-label="search"
          {...searchInput}
        />
      </div>
      <ErrorMessages />
      <ul className="list-group">
        {filterList.map((item) => (
          <li key={item.id} className="list-group-item">
            <Link to={`/view?id=${item.id}`} className="text-black text-decoration-none">
              <h6 className="m-0">{item.name}</h6>
              {!!item.description && <div className="small text-secondary mt-2">{item.description}</div>}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
};
