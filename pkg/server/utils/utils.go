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
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/siglens/siglens/pkg/config"
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
const METRIC_PREFIX string = "/metrics-explorer"

// This function reduces some boilerplate code by handling the logic for
// injecting orgId if necessary, or using the default.
func CallWithMyIdQuery(handler func(*fasthttp.RequestCtx, int64), ctx *fasthttp.RequestCtx) {
	orgId := int64(0)
	var err error
	if hook := hooks.GlobalHooks.GetOrgIdHookQuery; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			utils.SendUnauthorizedError(ctx, "Failed authorization", "", err)
			return
		}
	}

	handler(ctx, orgId)
}

func CallWithMyId(handler func(*fasthttp.RequestCtx, int64), ctx *fasthttp.RequestCtx) {
	orgId := int64(0)
	var err error
	if hook := hooks.GlobalHooks.GetOrgIdHook; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			utils.SendUnauthorizedError(ctx, "Failed authorization", "", err)
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

func GetMyIds() []int64 {
	if hook := hooks.GlobalHooks.GetIdsConditionHook; hook != nil {
		alreadyHandled, ids := hook()
		if alreadyHandled {
			return ids
		}
	}

	return []int64{0}
}

func GetTlsConfig(getCertificate func(*tls.ClientHelloInfo) (*tls.Certificate, error)) (*tls.Config, error) {
	cfg := &tls.Config{
		GetCertificate: getCertificate,
	}

	if config.IsMtlsEnabled() {
		systemPool, _ := x509.SystemCertPool()
		if systemPool == nil {
			systemPool = x509.NewCertPool()
		}

		clientCaPath := config.GetMtlsClientCaPath()
		customCa, err := os.ReadFile(clientCaPath)
		if err != nil {
			return nil, err
		}

		systemPool.AppendCertsFromPEM(customCa)

		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = systemPool
	}

	return cfg, nil
}
