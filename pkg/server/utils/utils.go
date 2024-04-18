// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package server_utils

import (
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

const ELASTIC_PREFIX string = "/elastic"
const OTSDB_PREFIX string = "/otsdb"
const INFLUX_PREFIX string = "/influx"
const PROMQL_PREFIX string = "/promql"
const OTLP_PREFIX string = "/otlp"
const API_PREFIX string = "/api"
const LOKI_PREFIX string = "/loki"
const HEROKU_ADDON_PREFIX string = "/heroku/resources"

// This function reduces some boilerplate code by handling the logic for
// injecting orgId if necessary, or using the default.
func CallWithOrgIdQuery(handler func(*fasthttp.RequestCtx, uint64), ctx *fasthttp.RequestCtx) {
	orgId := uint64(0)
	var err error
	if hook := hooks.GlobalHooks.GetOrgIdHookQuery; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			responsebody := make(map[string]interface{})
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			responsebody["error"] = err.Error()
			utils.WriteJsonResponse(ctx, responsebody)
			return
		}
	}

	handler(ctx, orgId)
}

func CallWithOrgId(handler func(*fasthttp.RequestCtx, uint64), ctx *fasthttp.RequestCtx) {
	orgId := uint64(0)
	var err error
	if hook := hooks.GlobalHooks.GetOrgIdHook; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			responsebody := make(map[string]interface{})
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			responsebody["error"] = err.Error()
			utils.WriteJsonResponse(ctx, responsebody)
			return
		}
	}

	handler(ctx, orgId)
}

func ExtractKibanaRequests(kibanaIndices []string, qid uint64) map[string]*structs.SegmentSearchRequest {
	ssr := make(map[string]*structs.SegmentSearchRequest)

	if hook := hooks.GlobalHooks.ExtractKibanaRequestsHook; hook != nil {
		interfaces := hook(kibanaIndices, qid)
		for k, v := range interfaces {
			ssr[k] = v.(*structs.SegmentSearchRequest)
		}
	}

	return ssr
}
