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
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/server"
	log "github.com/sirupsen/logrus"

	"github.com/fasthttp/router"
	"github.com/oklog/run"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/ingest"
	"github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/valyala/fasthttp"
)

type ingestionServerCfg struct {
	Config config.WebConfig
	Addr   string
	//	Log    *zap.Logger //ToDo implement debug logger
	ln     net.Listener
	router *router.Router
	debug  bool
}

var (
	corsAllowHeaders = "Access-Control-Allow-Origin, Access-Control-Allow-Methods, Access-Control-Max-Age, Access-Control-Allow-Credentials, Content-Type, Authorization, Origin, X-Requested-With , Accept"
	corsAllowMethods = "HEAD, GET, POST, PUT, DELETE, OPTIONS"
	corsAllowOrigin  = "*"
)

// ConstructHttpServer new fasthttp server
func ConstructIngestServer(cfg config.WebConfig, ServerAddr string) *ingestionServerCfg {
	return &ingestionServerCfg{
		Config: cfg,
		Addr:   ServerAddr,
		router: router.New(),
		debug:  true,
	}
}

func (hs *ingestionServerCfg) Close() {
	_ = hs.ln.Close()
}

func (hs *ingestionServerCfg) Run() (err error) {
	// Register all the method handlers here
	ingest.InitIngestionMetrics()
	writer.InitWriterNode()

	if !config.IsQueryNode() && config.IsIngestNode() {
		go query.InitQueryInfoRefresh(server_utils.GetMyIds)
	}

	hs.router.GET(server_utils.API_PREFIX+"/health", getHealthHandler())
	hs.router.POST(server_utils.API_PREFIX+"/sampledataset_bulk", hs.Recovery(sampleDatasetBulkHandler()))

	hs.router.GET("/config", hs.Recovery(getConfigHandler()))
	hs.router.POST("/config/reload", hs.Recovery(getConfigReloadHandler()))

	// elasticsearch endpoints
	hs.router.HEAD(server_utils.ELASTIC_PREFIX+"/", hs.Recovery(esGreetHandler()))
	hs.router.GET(server_utils.ELASTIC_PREFIX+"/", hs.Recovery(esGreetHandler()))
	hs.router.GET(server_utils.ELASTIC_PREFIX+"/_xpack", hs.Recovery(esGreetHandler()))
	hs.router.POST(server_utils.ELASTIC_PREFIX+"/_bulk", hs.Recovery(esPostBulkHandler()))
	hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(EsPutIndexHandler()))
	hs.router.HEAD(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(EsPutIndexHandler()))

	hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/_mapping", hs.Recovery(EsPutIndexHandler()))
	hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/_mapping/{docType}", hs.Recovery(EsPutIndexHandler()))

	// without the doctype (>7.x format)
	if strings.HasPrefix(*config.GetESVersion(), "7.") {
		hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/_doc/{_id}", hs.Recovery(esPutPostSingleDocHandler(false)))
		hs.router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_doc/{_id?}", hs.Recovery(esPutPostSingleDocHandler(false)))

		hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/_create/{_id}", hs.Recovery(esPutPostSingleDocHandler(false)))
		hs.router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_create/{_id}", hs.Recovery(esPutPostSingleDocHandler(false)))

		hs.router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_update/{_id}", hs.Recovery(esPutPostSingleDocHandler(true)))

	} else {
		// with the doctype (<7.x format)

		hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{_id}",
			hs.Recovery(esPutPostSingleDocHandler(false)))
		hs.router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{_id?}",
			hs.Recovery(esPutPostSingleDocHandler(false)))

		hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{_id}/_create",
			hs.Recovery(esPutPostSingleDocHandler(false)))
		hs.router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{_id}/_create",
			hs.Recovery(esPutPostSingleDocHandler(false)))

		hs.router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{_id}/_update",
			hs.Recovery(esPutPostSingleDocHandler(true)))
	}

	// Loki endpoints
	hs.router.POST(server_utils.LOKI_PREFIX+"/api/v1/push", hs.Recovery(lokiPostBulkHandler()))

	// Splunk Handlers
	hs.router.POST("/services/collector/event", hs.Recovery(splunkHecIngestHandler()))
	hs.router.GET("/services/collector/health", hs.Recovery(getHealthHandler()))
	hs.router.GET("/services/collector/health/1.0", hs.Recovery(getHealthHandler()))

	// OpenTSDB Handlers
	hs.router.PUT(server_utils.OTSDB_PREFIX+"/api/put", hs.Recovery(otsdbPutMetricsHandler()))
	hs.router.POST(server_utils.OTSDB_PREFIX+"/api/put", hs.Recovery(otsdbPutMetricsHandler()))

	// Prometheus Handlers
	hs.router.POST(server_utils.PROMQL_PREFIX+"/api/v1/write", hs.Recovery(prometheusPutMetricsHandler()))

	// OTLP Handlers
	hs.router.POST(server_utils.OTLP_PREFIX+"/v1/traces", hs.Recovery(otlpIngestTracesHandler()))
	hs.router.POST(server_utils.OTLP_PREFIX+"/v1/logs", hs.Recovery(otlpIngestLogsHandler()))
	hs.router.POST(server_utils.OTLP_PREFIX+"/v1/metrics", hs.Recovery(otlpIngestMetricsHandler()))

	if hook := hooks.GlobalHooks.ExtraIngestEndpointsHook; hook != nil {
		hook(hs.router, hs.Recovery)
	}

	hs.ln, err = net.Listen("tcp", hs.Addr)
	if err != nil {
		log.Errorf("ingestionServerCfg.Run: Failed to listen on %s, err=%v", hs.Addr, err)
		return err
	}

	s := &fasthttp.Server{
		Handler:            cors(hs.router.Handler),
		Name:               hs.Config.Name,
		ReadBufferSize:     hs.Config.ReadBufferSize,
		MaxConnsPerIP:      hs.Config.MaxConnsPerIP,
		MaxRequestsPerConn: hs.Config.MaxRequestsPerConn,
		MaxRequestBodySize: hs.Config.MaxRequestBodySize, //  100 << 20, // 100MB // 1024 * 4, // MaxRequestBodySize:
		Concurrency:        hs.Config.Concurrency,
	}

	if config.IsTlsEnabled() {
		certReloader, err := server.NewCertReloader(config.GetTLSCertificatePath(), config.GetTLSPrivateKeyPath())
		if err != nil {
			log.Fatalf("Run: error loading TLS certificate: %v, err=%v", config.GetTLSCertificatePath(), err)
			return err
		}

		cfg, err := server_utils.GetTlsConfig(certReloader.GetCertificate)
		if err != nil {
			log.Fatalf("Run: error getting TLS config; err=%v", err)
			return err
		}

		hs.ln = tls.NewListener(hs.ln, cfg)
	}

	var g run.Group
	g.Add(func() error {
		return s.Serve(hs.ln)
	}, func(e error) {
		log.Errorf("ingestionServerCfg.Run: Failed to serve on %s, err=%v", hs.Addr, e)
		_ = hs.ln.Close()
	})

	return g.Run()
}

func (hs *ingestionServerCfg) RunSafeServer() (err error) {
	hs.router.GET("/health", hs.Recovery(getSafeHealthHandler()))
	hs.ln, err = net.Listen("tcp", hs.Addr)
	if err != nil {
		log.Errorf("ingestionServerCfg.RunSafeServer: Failed to listen on %s, err=%v", hs.Addr, err)
		return err
	}

	s := &fasthttp.Server{
		Handler:            cors(hs.router.Handler),
		Name:               hs.Config.Name,
		ReadBufferSize:     hs.Config.ReadBufferSize,
		MaxConnsPerIP:      hs.Config.MaxConnsPerIP,
		MaxRequestsPerConn: hs.Config.MaxRequestsPerConn,
		MaxRequestBodySize: hs.Config.MaxRequestBodySize, //  100 << 20, // 100MB // 1024 * 4, // MaxRequestBodySize:
		Concurrency:        hs.Config.Concurrency,
	}

	log.Infof("Starting Ingestion Server on safe mode...")
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			log.Infof("Siglens Ingestion Server has started in safe mode...")
		}
	}()

	// run fasthttp server
	var g run.Group
	g.Add(func() error {
		return s.Serve(hs.ln)
	}, func(e error) {
		log.Errorf("ingestionServerCfg.RunSafeServer: Failed to serve on %s, err=%v", hs.Addr, e)
		_ = hs.ln.Close()
	})
	return g.Run()
}

func cors(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Headers", corsAllowHeaders)
		ctx.Response.Header.Set("Access-Control-Allow-Methods", corsAllowMethods)
		ctx.Response.Header.Set("Access-Control-Allow-Origin", corsAllowOrigin)
		ctx.Response.Header.Set("Access-Control-Allow-Credentials", "true")
		next(ctx)
	}
}
