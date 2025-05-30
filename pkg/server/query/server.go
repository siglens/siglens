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

package queryserver

import (
	"crypto/tls"
	htmltemplate "html/template"
	"net"
	texttemplate "text/template"
	"time"

	"github.com/fasthttp/router"
	"github.com/oklog/run"
	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/server"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	tracing "github.com/siglens/siglens/pkg/tracing"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/pprofhandler"
)

type queryserverCfg struct {
	Config config.WebConfig
	Addr   string
	//	Log    *zap.Logger //ToDo implement debug logger
	ln            net.Listener
	Router        *router.Router
	staticHandler fasthttp.RequestHandler
	debug         bool
}

var (
	corsAllowHeaders = "Access-Control-Allow-Origin, Access-Control-Request-Method, Access-Control-Allow-Methods, Access-Control-Max-Age, Content-Type, Authorization, Origin, X-Requested-With , Accept"
	corsAllowMethods = "HEAD,GET,POST,PUT,DELETE,OPTIONS,UPGRADE"
	corsAllowOrigin  = "*"
)

// ConstructHttpServer new fasthttp server
func ConstructQueryServer(cfg config.WebConfig, ServerAddr string) *queryserverCfg {
	staticFs := fasthttp.FS{
		Root:           "./static",
		IndexNames:     []string{"index.html"},
		Compress:       config.ShouldCompressStaticFiles(),
		CompressBrotli: config.ShouldCompressStaticFiles(),
	}

	s := &queryserverCfg{
		Config:        cfg,
		Addr:          ServerAddr,
		Router:        router.New(),
		staticHandler: staticFs.NewRequestHandler(),
		debug:         true,
	}
	return s
}

func (hs *queryserverCfg) Close() {
	_ = hs.ln.Close()
}

func (hs *queryserverCfg) Run(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) error {
	if config.IsTracingEnabled() {
		cleanup := tracing.InitTracing(config.GetTracingServiceName() + ":query")
		defer cleanup()
	}

	query.InitQueryMetrics()

	err := query.InitQueryNode(server_utils.GetMyIds, server_utils.ExtractKibanaRequests)
	if err != nil {
		log.Errorf("Failed to initialize query node: %v", err)
		return err
	}

	alertsHandler.InitAlertingService(server_utils.GetMyIds)
	alertsHandler.InitMinionSearchService(server_utils.GetMyIds)

	hs.Router.GET("/{filename}.html", func(ctx *fasthttp.RequestCtx) {
		renderHtmlTemplate(ctx, htmlTemplate)
	})
	hs.Router.GET("/js/{filename}.js", func(ctx *fasthttp.RequestCtx) {
		renderJavaScriptTemplate(ctx, textTemplate)
	})
	hs.Router.GET(server_utils.API_PREFIX+"/search/live_tail", tracing.TraceMiddleware(hs.Recovery(liveTailHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/search/live_tail", tracing.TraceMiddleware(hs.Recovery(liveTailHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/search", tracing.TraceMiddleware(hs.Recovery(pipeSearchHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/search/{dbPanel-id}", tracing.TraceMiddleware(hs.Recovery(dashboardPipeSearchHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/search/ws", tracing.TraceMiddleware(hs.Recovery(pipeSearchWebsocketHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/search/ws", tracing.TraceMiddleware(hs.Recovery(pipeSearchWebsocketHandler())))

	hs.Router.POST(server_utils.API_PREFIX+"/sampledataset_bulk", tracing.TraceMiddleware(hs.Recovery(sampleDatasetBulkHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/sampletraces", tracing.TraceMiddleware(hs.Recovery(sampleTracesHandler())))

	// common routes

	hs.Router.GET(server_utils.API_PREFIX+"/health", tracing.TraceMiddleware(getHealthHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/config", tracing.TraceMiddleware(hs.Recovery(getConfigHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/config/reload", tracing.TraceMiddleware(hs.Recovery(getConfigReloadHandler())))

	// elasticsearch routes - common to both ingest and query
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/", hs.Recovery(esGreetHandler()))

	// elasticsearch routes - specific to query
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/search", hs.Recovery(esGetSearchHandler()))
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/{indexName}/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_doc/_search", hs.Recovery(esGetSearchHandler()))

	hs.Router.DELETE(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(esDeleteIndexHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/deleteIndex/{indexName}", hs.Recovery(esDeleteIndexHandler()))

	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/_search", hs.Recovery(esGetSearchHandler()))

	hs.Router.HEAD(server_utils.ELASTIC_PREFIX+"/", hs.Recovery(headHandler()))

	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{idVal}", hs.Recovery(esGetSingleDocHandler()))
	hs.Router.HEAD(server_utils.ELASTIC_PREFIX+"/{indexName}/{docType}/{idVal}", hs.Recovery(esGetSingleDocHandler()))

	// aliases api
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/{indexName}/_alias/{aliasName}", hs.Recovery(esGetIndexAliasesHandler()))
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/_alias/{aliasName}", hs.Recovery(esGetAliasHandler()))
	hs.Router.HEAD(server_utils.ELASTIC_PREFIX+"/_alias/{aliasName}", hs.Recovery(esGetAliasHandler()))
	hs.Router.HEAD(server_utils.ELASTIC_PREFIX+"/{indexName}/_alias/{aliasName?}", hs.Recovery(esGetIndexAliasesHandler()))

	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/_aliases", hs.Recovery(esPostAliasesHandler()))

	hs.Router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/_alias/{aliasName}", hs.Recovery(esPutIndexAliasHandler()))
	hs.Router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}/_aliases/{aliasName}", hs.Recovery(esPutIndexAliasHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_alias/{aliasName}", hs.Recovery(esPutIndexAliasHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_aliases/{aliasName}", hs.Recovery(esPutIndexAliasHandler()))

	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/_aliases", hs.Recovery(esGetAllAliasesHandler()))
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/_cat/aliases", hs.Recovery(esGetAllAliasesHandler()))

	hs.Router.HEAD(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(esGetIndexAliasExistsHandler()))
	/*
		hs.router.DELETE(ELASTIC_PREFIX+"/{indexName}/_alias/{aliasName}", hs.Recovery(esDeleteAliasHandler()))
	*/

	// splunk endpoint
	hs.Router.GET("/services/collector/health", hs.Recovery(getHealthHandler()))
	hs.Router.GET("/services/collector/health/1.0", hs.Recovery(getHealthHandler()))

	// jaeger
	hs.Router.GET(server_utils.JAEGER_PREFIX+"/api/services", hs.Recovery(getServicesHandler()))
	hs.Router.GET(server_utils.JAEGER_PREFIX+"/api/services/{serviceName}/operations", hs.Recovery(getOperationsHandler()))
	hs.Router.GET(server_utils.JAEGER_PREFIX+"/api/dependencies", hs.Recovery(getDependenciesHandler()))
	hs.Router.GET(server_utils.JAEGER_PREFIX+"/api/traces", hs.Recovery(getTracesHandler()))

	// OTSDB query endpoint
	hs.Router.GET(server_utils.OTSDB_PREFIX+"/api/query", hs.Recovery(otsdbMetricQueryHandler()))
	hs.Router.POST(server_utils.OTSDB_PREFIX+"/api/query", hs.Recovery(otsdbMetricQueryHandler()))
	hs.Router.POST(server_utils.OTSDB_PREFIX+"/api/v1/query/exp", hs.Recovery(otsdbMetricQueryExpHandler()))

	// prometheus query endpoint
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/v1/query", hs.Recovery(promqlMetricsInstantQueryHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/query", hs.Recovery(promqlMetricsInstantQueryHandler()))
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/ui/query", hs.Recovery(uiMetricsSearchHandler()))
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/v1/query_range", hs.Recovery(promqlMetricsRangeQueryHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/query_range", hs.Recovery(promqlMetricsRangeQueryHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/status/buildinfo", hs.Recovery(promqlBuildInfoHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/labels", hs.Recovery(promqlGetLabelsHandler()))
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/v1/labels", hs.Recovery(promqlGetLabelsHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/label/{labelName}/values", hs.Recovery(promqlGetLabelValuesHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/series", hs.Recovery(promqlGetSeriesByLabelHandler()))
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/v1/series", hs.Recovery(promqlGetSeriesByLabelHandler()))

	// metric explorer endpoint
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/metric_names", hs.Recovery(getAllMetricNamesHandler()))
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/all_tags", hs.Recovery(getAllMetricTagsHandler()))
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/timeseries", hs.Recovery(getMetricTimeSeriesHandler()))
	hs.Router.GET(server_utils.METRIC_PREFIX+"/api/v1/functions", hs.Recovery(getMetricFunctionsHandler()))
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/series-cardinality", hs.Recovery(getMetricSeriesCardinalityHandler()))
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/tag-keys-with-most-series", hs.Recovery(getTagKeysWithMostSeriesHandler()))
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/tag-pairs-with-most-series", hs.Recovery(getTagPairsWithMostSeriesHandler()))
	hs.Router.POST(server_utils.METRIC_PREFIX+"/api/v1/tag-keys-with-most-values", hs.Recovery(getTagKeysWithMostValuesHandler()))

	// search api Handlers
	hs.Router.POST(server_utils.API_PREFIX+"/echo", tracing.TraceMiddleware(hs.Recovery(pipeSearchHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/listIndices", tracing.TraceMiddleware(hs.Recovery(listIndicesHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/listColumnNames", tracing.TraceMiddleware(hs.Recovery(listColumnNamesHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/clusterStats", tracing.TraceMiddleware(hs.Recovery(getClusterStatsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/usageStats", tracing.TraceMiddleware(hs.Recovery(getUsageStatsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/usersavedqueries/save", tracing.TraceMiddleware(hs.Recovery(saveUserSavedQueriesHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/usersavedqueries/getall", tracing.TraceMiddleware(hs.Recovery(getUserSavedQueriesAllHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/usersavedqueries/deleteone/{qname}", tracing.TraceMiddleware(hs.Recovery(deleteUserSavedQueryHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/usersavedqueries/{qname}", tracing.TraceMiddleware(hs.Recovery(SearchUserSavedQueryHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/pqs/clear", tracing.TraceMiddleware(hs.Recovery(postPqsClearHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/pqs/delete", tracing.TraceMiddleware(hs.Recovery(postPqsDeleteHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs/get", tracing.TraceMiddleware(hs.Recovery(getPqsEnabledHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/pqs/aggs", tracing.TraceMiddleware(hs.Recovery(postPqsAggColsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/pqs/update", tracing.TraceMiddleware(hs.Recovery(postPqsHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs", tracing.TraceMiddleware(hs.Recovery(getPqsHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs/{pqid}", tracing.TraceMiddleware(hs.Recovery(getPqsByIdHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/dashboards/create", tracing.TraceMiddleware(hs.Recovery(createDashboardHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/dashboards/update", tracing.TraceMiddleware(hs.Recovery(updateDashboardHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/{dashboard-id}", tracing.TraceMiddleware(hs.Recovery(getDashboardIdHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/delete/{dashboard-id}", tracing.TraceMiddleware(hs.Recovery(deleteDashboardHandler())))
	hs.Router.PUT(server_utils.API_PREFIX+"/dashboards/favorite/{dashboard-id}", tracing.TraceMiddleware(hs.Recovery(favoriteDashboardHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/list", tracing.TraceMiddleware(hs.Recovery(listAllDashboardsHandler())))
	// folders api endpoints
	hs.Router.POST(server_utils.API_PREFIX+"/dashboards/folders/create", tracing.TraceMiddleware(hs.Recovery(createFolderHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/folders/{folder-id}", tracing.TraceMiddleware(hs.Recovery(getFolderContentsHandler())))
	hs.Router.PUT(server_utils.API_PREFIX+"/dashboards/folders/{folder-id}", tracing.TraceMiddleware(hs.Recovery(updateFolderHandler())))
	hs.Router.DELETE(server_utils.API_PREFIX+"/dashboards/folders/{folder-id}", tracing.TraceMiddleware(hs.Recovery(deleteFolderHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/folders/{folder-id}/count", tracing.TraceMiddleware(hs.Recovery(getFolderNestedCountHandler())))

	hs.Router.GET(server_utils.API_PREFIX+"/version/info", tracing.TraceMiddleware(hs.Recovery(getVersionHandler())))

	// alerting api endpoints
	hs.Router.POST(server_utils.API_PREFIX+"/alerts/create", hs.Recovery(createAlertHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/alerts/{alertID}", hs.Recovery(getAlertHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/allalerts", hs.Recovery(getAllAlertsHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/alerts/update", hs.Recovery(updateAlertHandler()))
	hs.Router.DELETE(server_utils.API_PREFIX+"/alerts/delete", hs.Recovery(deleteAlertHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/alerts/{alertID}/history", hs.Recovery(alertHistoryHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/alerts/createContact", hs.Recovery(createContactHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/alerts/allContacts", hs.Recovery(getAllContactsHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/alerts/updateContact", hs.Recovery(updateContactHandler()))
	hs.Router.DELETE(server_utils.API_PREFIX+"/alerts/deleteContact", hs.Recovery(deleteContactHandler()))
	hs.Router.PUT(server_utils.API_PREFIX+"/alerts/silenceAlert", hs.Recovery(silenceAlertHandler()))
	hs.Router.PUT(server_utils.API_PREFIX+"/alerts/unsilenceAlert", hs.Recovery(unsilenceAlertHandler()))

	hs.Router.POST(server_utils.API_PREFIX+"/alerts/testContactPoint", hs.Recovery(testContactPointHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/minionsearch/allMinionSearches", hs.Recovery(getAllMinionSearchesHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/minionsearch/createMinionSearches", hs.Recovery(createMinionSearchHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/minionsearch/{alertID}", hs.Recovery(getMinionSearchHandler()))

	// tracing api endpoints
	hs.Router.POST(server_utils.API_PREFIX+"/traces/search", tracing.TraceMiddleware(hs.Recovery(searchTracesHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/dependencies", tracing.TraceMiddleware(hs.Recovery(getDependencyGraphHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/generate-dep-graph", hs.Recovery(generateDependencyGraphHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/ganttChart", tracing.TraceMiddleware(hs.Recovery(ganttChartHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/span/ganttChart", tracing.TraceMiddleware(hs.Recovery(spanGanttChartHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/count", tracing.TraceMiddleware(hs.Recovery((totalTracesHandler()))))
	// query server should still setup ES APIs for Kibana integration
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/_bulk", hs.Recovery(esPostBulkHandler()))
	hs.Router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(esPutIndexHandler()))

	hs.Router.POST(server_utils.API_PREFIX+"/lookup-upload", hs.Recovery(uploadLookupFileHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/lookup-files", hs.Recovery(getAllLookupFilesHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/lookup-files/{lookupFilename}", hs.Recovery(getLookupFileHandler()))
	hs.Router.DELETE(server_utils.API_PREFIX+"/lookup-files/{lookupFilename}", hs.Recovery(deleteLookupFileHandler()))

	hs.Router.GET(server_utils.API_PREFIX+"/system-info", tracing.TraceMiddleware(hs.Recovery(getSystemInfoHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/inode-stats", tracing.TraceMiddleware(hs.Recovery(getInodesStatsHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/query-stats", hs.Recovery(getQueryStatsHandler()))

	hs.Router.POST(server_utils.API_PREFIX+"/update-query-timeout", hs.Recovery(UpdateQueryTimeoutHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/get-query-timeout", hs.Recovery(GetQueryTimeoutHandler()))

	hs.Router.POST(server_utils.API_PREFIX+"/sort-columns", hs.Recovery(setSortColumnsHandler()))

	hs.Router.GET(server_utils.API_PREFIX+"/collect-diagnostics", hs.Recovery(collectDiagnosticsHandler()))

	if config.IsPProfEnabled() {
		hs.Router.GET("/debug/pprof/{profile:*}", pprofhandler.PprofHandler)
	}

	if hook := hooks.GlobalHooks.ExtraQueryEndpointsHook; hook != nil {
		err := hook(hs.Router, hs.Recovery)
		if err != nil {
			log.Errorf("Run: error in ExtraQueryEndpointsHook: %v", err)
			return err
		}
	}

	if hook := hooks.GlobalHooks.ServeStaticHook; hook != nil {
		hook(hs.Router, htmlTemplate)
	} else {
		hook = func(router *router.Router, htmlTemplate *htmltemplate.Template) {
			router.GET("/{filepath:*}", func(ctx *fasthttp.RequestCtx) {
				filepath := ctx.UserValue("filepath").(string)
				if filepath == "" {
					// Render index.html and send that.
					ctx.Response.Header.Set("Content-Type", "text/html; charset=utf-8")
					err := htmlTemplate.ExecuteTemplate(ctx, "index.html", hooks.GlobalHooks.HtmlSnippets)
					if err != nil {
						log.Fatalf("serveStatic: error executing index.html template: %v", err)
					}

					return
				}

				hs.staticHandler(ctx)
			})
		}

		hook(hs.Router, htmlTemplate)
	}

	hs.ln, err = net.Listen("tcp", hs.Addr)
	if err != nil {
		return err
	}

	s := &fasthttp.Server{
		Handler:            cors(hs.Router.Handler),
		Name:               hs.Config.Name,
		ReadBufferSize:     hs.Config.ReadBufferSize,
		MaxConnsPerIP:      hs.Config.MaxConnsPerIP,
		MaxRequestsPerConn: hs.Config.MaxRequestsPerConn,
		MaxRequestBodySize: hs.Config.MaxRequestBodySize, //  100 << 20, // 100MB // 1000 * 4, // MaxRequestBodySize:
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
		log.Errorf("queryServerCfg.Run: Failed to serve on %s, err=%v", hs.Addr, e)
		_ = hs.ln.Close()
	})

	return g.Run()
}

func renderHtmlTemplate(ctx *fasthttp.RequestCtx, tpl *htmltemplate.Template) {
	filename := utils.ExtractParamAsString(ctx.UserValue("filename"))
	ctx.Response.Header.Set("Content-Type", "text/html; charset=utf-8")
	err := tpl.ExecuteTemplate(ctx, filename+".html", hooks.GlobalHooks.HtmlSnippets)
	if err != nil {
		log.Errorf("renderHtmlTemplate: unable to execute template, err: %v", err.Error())
		return
	}
}

func renderJavaScriptTemplate(ctx *fasthttp.RequestCtx, tpl *texttemplate.Template) {
	filename := utils.ExtractParamAsString(ctx.UserValue("filename"))
	ctx.Response.Header.Set("Content-Type", "application/javascript; charset=utf-8")
	err := tpl.ExecuteTemplate(ctx, filename+".js", hooks.GlobalHooks.JsSnippets)
	if err != nil {
		log.Errorf("renderJavaScriptTemplate: unable to execute template, err: %v", err.Error())
		return
	}
}

func (hs *queryserverCfg) RunSafeServer() error {
	hs.Router.GET("/health", hs.Recovery(getSafeHealthHandler()))
	var err error
	hs.ln, err = net.Listen("tcp", hs.Addr)
	if err != nil {
		return err
	}

	s := &fasthttp.Server{
		Handler:            cors(hs.Router.Handler),
		Name:               hs.Config.Name,
		ReadBufferSize:     hs.Config.ReadBufferSize,
		MaxConnsPerIP:      hs.Config.MaxConnsPerIP,
		MaxRequestsPerConn: hs.Config.MaxRequestsPerConn,
		MaxRequestBodySize: hs.Config.MaxRequestBodySize, //  100 << 20, // 100MB // 1000 * 4, // MaxRequestBodySize:
		Concurrency:        hs.Config.Concurrency,
	}

	log.Infof("Starting Ingestion Server on safe mode...")
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			log.Infof("SigLens Ingestion Server has started in safe mode...")
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
