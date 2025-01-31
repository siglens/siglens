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
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/siglens/siglens/pkg/config"
	esutils "github.com/siglens/siglens/pkg/es/utils"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/health"
	"github.com/siglens/siglens/pkg/hooks"
	influxquery "github.com/siglens/siglens/pkg/influx/query"
	influxwriter "github.com/siglens/siglens/pkg/influx/writer"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/integrations/loki"
	otsdbwriter "github.com/siglens/siglens/pkg/integrations/otsdb/writer"
	prometheuswriter "github.com/siglens/siglens/pkg/integrations/prometheus/ingest"
	"github.com/siglens/siglens/pkg/integrations/splunk"
	"github.com/siglens/siglens/pkg/otlp"
	"github.com/siglens/siglens/pkg/sampledataset"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var diskUsageExceeded atomic.Bool

func MonitorDiskUsage() {
	for {
		usage, err := getDiskUsagePercent()
		if err != nil {
			time.Sleep(30 * time.Second)
			continue
		}
		if usage >= config.GetDataDiskThresholdPercent() {
			log.Errorf("MonitorDiskUsage: Disk usage (%+v%%) exceeded the dataDiskThresholdPercent (%+v%%)", usage, config.GetDataDiskThresholdPercent())
			diskUsageExceeded.Store(true)
		} else {
			diskUsageExceeded.Store(false)
		}
		time.Sleep(30 * time.Second)
	}
}

func getDiskUsagePercent() (uint64, error) {
	s, err := disk.Usage(config.GetDataPath())
	if err != nil {
		log.Errorf("getDiskUsagePercent: Error getting disk usage for the disk data path=%v, err=%v", config.GetDataPath(), err)
		return 0, err
	}
	percentUsed := (s.Used * 100) / s.Total
	return percentUsed, nil
}

func canIngest() bool {
	return !diskUsageExceeded.Load()
}
func esPostBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if !canIngest() {
			utils.SendErrorWithoutLogging(ctx, "Ingestion request rejected due to no storage available", nil)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)

		if hook := hooks.GlobalHooks.KibanaIngestHandlerHook; hook != nil {
			hook(ctx)
		} else {
			eswriter.ProcessBulkRequest(ctx, 0, false)
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
		serverutils.CallWithMyId(splunk.ProcessSplunkHecIngestRequest, ctx)
	}
}

func EsPutIndexHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(eswriter.ProcessPutIndex, ctx)
	}
}

func esPutPostSingleDocHandler(update bool) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)
		eswriter.ProcessPutPostSingleDocRequest(ctx, update, 0)
	}
}

func otsdbPutMetricsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(otsdbwriter.PutMetrics, ctx)
	}
}

func influxPutMetricsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(influxwriter.PutMetrics, ctx)
	}
}

func influxQueryGetHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(influxquery.GetQueryHandler, ctx)
	}
}

func influxQueryPostHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(influxquery.PostQueryHandler, ctx)
	}
}
func esGreetHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		esutils.ProcessGreetHandler(ctx)
	}
}

func prometheusPutMetricsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(prometheuswriter.PutMetrics, ctx)
	}
}

func otlpIngestTracesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(otlp.ProcessTraceIngest, ctx)
	}
}

func sampleDatasetBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)
		serverutils.CallWithMyId(sampledataset.ProcessSyntheicDataRequest, ctx)
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
		serverutils.CallWithMyId(loki.ProcessLokiLogsIngestRequest, ctx)
	}
}
