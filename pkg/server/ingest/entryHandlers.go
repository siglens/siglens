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

package ingestserver

import (
	"github.com/siglens/siglens/pkg/config"
	esutils "github.com/siglens/siglens/pkg/es/utils"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/health"
	"github.com/siglens/siglens/pkg/hooks"
	influxwriter "github.com/siglens/siglens/pkg/influx/writer"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/integrations/loki"
	otsdbwriter "github.com/siglens/siglens/pkg/integrations/otsdb/writer"
	prometheuswriter "github.com/siglens/siglens/pkg/integrations/prometheus/ingest"
	"github.com/siglens/siglens/pkg/integrations/splunk"
	"github.com/siglens/siglens/pkg/otlp"
	"github.com/siglens/siglens/pkg/sampledataset"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/valyala/fasthttp"
)

func processKibanaIngestRequest(ctx *fasthttp.RequestCtx, request map[string]interface{},
	indexNameConverted string, updateArg bool, idVal string, tsNow uint64, myid uint64) error {
	return nil
}

func esPostBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)

		if hook := hooks.GlobalHooks.KibanaIngestHandlerHook; hook != nil {
			hook(ctx)
		} else {
			eswriter.ProcessBulkRequest(ctx, 0, processKibanaIngestRequest)
		}
	}
}

func getHealthHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessGetHealth(ctx)
	}
}

func getSafeHealthHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessSafeHealth(ctx)
	}
}

func splunkHecIngestHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)
		serverutils.CallWithOrgId(splunk.ProcessSplunkHecIngestRequest, ctx)
	}
}

func EsPutIndexHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgId(eswriter.ProcessPutIndex, ctx)
	}
}

func otsdbPutMetricsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgId(otsdbwriter.PutMetrics, ctx)
	}
}

func influxPutMetricsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgId(influxwriter.PutMetrics, ctx)
	}
}

func esGreetHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		esutils.ProcessGreetHandler(ctx)
	}
}

func prometheusPutMetricsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		prometheuswriter.PutMetrics(ctx)
	}
}

func otlpIngestTracesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		otlp.ProcessTraceIngest(ctx)
	}
}

func sampleDatasetBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)
		serverutils.CallWithOrgId(sampledataset.ProcessSyntheicDataRequest, ctx)
	}
}

func postSetconfigHandler(persistent bool) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		config.ProcessSetConfig(persistent, ctx)
	}
}

func getConfigHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		config.ProcessGetConfigAsJson(ctx)
	}
}

func getConfigReloadHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		config.ProcessForceReadConfig(ctx)
	}
}

func lokiPostBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)
		serverutils.CallWithOrgId(loki.ProcessLokiLogsIngestRequest, ctx)
	}
}
