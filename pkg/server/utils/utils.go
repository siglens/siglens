// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package server_utils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

const ELASTIC_PREFIX string = "/elastic"
const OTSDB_PREFIX string = "/otsdb"
const PROMQL_PREFIX string = "/promql"
const OTLP_PREFIX string = "/otlp"
const API_PREFIX string = "/api"
const LOKI_PREFIX string = "/loki"
const HEROKU_ADDON_PREFIX string = "/heroku/resources"
const METRIC_PREFIX string = "/metrics-explorer"
const JAEGER_PREFIX string = "/jaeger"

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
		if clientCaPath == "" {
			return nil, fmt.Errorf("mTLS is enabled but the client CA path is not set")
		}

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
