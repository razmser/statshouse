// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GetParams } from './GetParams';
import { apiFetch } from './api';

const ApiDashboardEndpoint = '/api/dashboard';

/**
 * Response endpoint api/dashboard
 */
export type ApiDashboard = {
  data: DashboardInfo;
};

/**
 * Get params endpoint api/dashboard
 */
export type ApiDashboardGet = {
  [GetParams.dashboardID]: string;
};

/**
 * Post params endpoint api/dashboard
 */
export type ApiDashboardPost = DashboardInfo;

/**
 * Put params endpoint api/dashboard
 */
export type ApiDashboardPut = DashboardInfo;

export type DashboardInfo = {
  dashboard: DashboardMetaInfo;
  delete_mark: boolean;
};

export type DashboardMetaInfo = {
  dashboard_id: number;
  name: string;
  description: string;
  version?: number;
  update_time: number;
  deleted_time: number;
  data: Record<string, unknown>;
};

export async function apiDashboardFetch(params: ApiDashboardGet, keyRequest?: unknown) {
  return await apiFetch<ApiDashboard>({ url: ApiDashboardEndpoint, get: params, keyRequest });
}
