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

package ingestserver

import (
	"net"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	log "github.com/sirupsen/logrus"

	"github.com/fasthttp/router"
	"github.com/oklog/run"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/ingest"
	"github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/pprofhandler"
	"github.com/valyala/fasthttp/reuseport"
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

	s := &ingestionServerCfg{
		Config: cfg,
		Addr:   ServerAddr,
		router: router.New(),
		debug:  true,
	}
	return s
}

func (hs *ingestionServerCfg) Close() {
	_ = hs.ln.Close()
}

func getMyIds() []uint64 {
	myids := make([]uint64, 1)
	myids[0] = 0
	return myids
}

func (hs *ingestionServerCfg) Run() (err error) {

	//Register all the method handlers here
	ingest.InitIngestionMetrics()
	writer.InitWriterNode()

	if !config.IsQueryNode() && config.IsIngestNode() {
		go query.InitQueryInfoRefresh(getMyIds)
	}

	hs.router.GET(server_utils.API_PREFIX+"/health", hs.Recovery(getHealthHandler()))
	hs.router.POST(server_utils.API_PREFIX+"/sampledataset_bulk", hs.Recovery(sampleDatasetBulkHandler()))

	hs.router.POST("/setconfig/transient", hs.Recovery(postSetconfigHandler(false)))
	hs.router.POST("/setconfig/persistent", hs.Recovery(postSetconfigHandler(true)))
	hs.router.GET("/config", hs.Recovery(getConfigHandler()))
	hs.router.POST("/config/reload", hs.Recovery(getConfigReloadHandler()))

	//elasticsearch endpoints
	hs.router.GET(server_utils.ELASTIC_PREFIX+"/", hs.Recovery(esGreetHandler()))
	hs.router.POST(server_utils.ELASTIC_PREFIX+"/_bulk", hs.Recovery(esPostBulkHandler()))
	hs.router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(esPutIndexHandler()))

	// Loki endpoints
	hs.router.POST(server_utils.LOKI_PREFIX+"/api/v1/push", hs.Recovery(lokiPostBulkHandler()))

	// Splunk Handlers
	hs.router.POST(server_utils.SPLUNK_PREFIX+"/services/collector/event", hs.Recovery(splunkHecIngestHandler()))
	hs.router.GET(server_utils.SPLUNK_PREFIX+"/services/collector/health", hs.Recovery(getHealthHandler()))
	hs.router.GET(server_utils.SPLUNK_PREFIX+"/services/collector/health/1.0", hs.Recovery(getHealthHandler()))

	// OpenTSDB Handlers
	hs.router.PUT(server_utils.OTSDB_PREFIX+"/api/put", hs.Recovery(otsdbPutMetricsHandler()))
	hs.router.POST(server_utils.OTSDB_PREFIX+"/api/put", hs.Recovery(otsdbPutMetricsHandler()))

	// Influx Handlers
	hs.router.POST(server_utils.INFLUX_PREFIX+"/api/v2/write", hs.Recovery(influxPutMetricsHandler()))

	// Prometheus Handlers
	hs.router.POST(server_utils.PROMQL_PREFIX+"/api/v1/write", hs.Recovery(prometheusPutMetricsHandler()))

	if config.IsDebugMode() {
		hs.router.GET("/debug/pprof/{profile:*}", pprofhandler.PprofHandler)
	}
	hs.ln, err = reuseport.Listen("tcp4", hs.Addr)
	if err != nil {
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

	// run fasthttp server
	var g run.Group
	g.Add(func() error {
		return s.Serve(hs.ln)
	}, func(e error) {
		_ = hs.ln.Close()
	})
	return g.Run()
}

func (hs *ingestionServerCfg) RunSafeServer() (err error) {
	hs.router.GET("/health", hs.Recovery(getSafeHealthHandler()))
	hs.ln, err = reuseport.Listen("tcp4", hs.Addr)
	if err != nil {
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
