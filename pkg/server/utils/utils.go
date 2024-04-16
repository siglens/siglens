/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
