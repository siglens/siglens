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

package queryserver

import (
	"crypto/tls"
	"fmt"
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
	ln     net.Listener
	lnTls  net.Listener
	Router *router.Router
	debug  bool
}

var (
	corsAllowHeaders = "Access-Control-Allow-Origin, Access-Control-Request-Method, Access-Control-Allow-Methods, Access-Control-Max-Age, Content-Type, Authorization, Origin, X-Requested-With , Accept"
	corsAllowMethods = "HEAD,GET,POST,PUT,DELETE,OPTIONS,UPGRADE"
	corsAllowOrigin  = "*"
)

// ConstructHttpServer new fasthttp server
func ConstructQueryServer(cfg config.WebConfig, ServerAddr string) *queryserverCfg {

	s := &queryserverCfg{
		Config: cfg,
		Addr:   ServerAddr,
		Router: router.New(),
		debug:  true,
	}
	return s
}

func (hs *queryserverCfg) Close() {
	_ = hs.ln.Close()
}

func getMyIds() []uint64 {
	myids := make([]uint64, 1)

	alreadyHandled := false
	if hook := hooks.GlobalHooks.GetIdsConditionHook; hook != nil {
		alreadyHandled = hook(myids)
	}

	if !alreadyHandled {
		myids[0] = 0
	}

	return myids
}

func (hs *queryserverCfg) Run(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) error {
	if config.IsTracingEnabled() {
		cleanup := tracing.InitTracing(config.GetTracingServiceName() + ":query")
		defer cleanup()
	}

	query.InitQueryMetrics()

	alertsHandler.InitAlertingService(getMyIds)
	alertsHandler.InitMinionSearchService(getMyIds)

	err := query.InitQueryNode(getMyIds, server_utils.ExtractKibanaRequests)
	if err != nil {
		log.Errorf("Failed to initialize query node: %v", err)
		return err
	}

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

	// common routes

	hs.Router.GET(server_utils.API_PREFIX+"/health", tracing.TraceMiddleware(hs.Recovery(getHealthHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/setconfig/transient", hs.Recovery(postSetconfigHandler(false)))
	hs.Router.POST(server_utils.API_PREFIX+"/setconfig/persistent", hs.Recovery(postSetconfigHandler(true)))
	hs.Router.GET(server_utils.API_PREFIX+"/config", tracing.TraceMiddleware(hs.Recovery(getConfigHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/config/reload", tracing.TraceMiddleware(hs.Recovery(getConfigReloadHandler())))

	//elasticsearch routes - common to both ingest and query
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/", hs.Recovery(esGreetHandler()))

	//elasticsearch routes - specific to query
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/search", hs.Recovery(esGetSearchHandler()))
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.GET(server_utils.ELASTIC_PREFIX+"/{indexName}/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_search", hs.Recovery(esGetSearchHandler()))
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/{indexName}/_doc/_search", hs.Recovery(esGetSearchHandler()))

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

	//loki endpoint
	hs.Router.GET(server_utils.LOKI_PREFIX+"/api/v1/labels", hs.Recovery(lokiLabelsHandler()))
	hs.Router.GET(server_utils.LOKI_PREFIX+"/api/v1/label/{labelName}/values", hs.Recovery(lokiLabelValueHandler()))
	hs.Router.GET(server_utils.LOKI_PREFIX+"/api/v1/query", hs.Recovery(lokiQueryHandler()))
	hs.Router.GET(server_utils.LOKI_PREFIX+"/api/v1/query_range", hs.Recovery(lokiQueryHandler()))
	hs.Router.GET(server_utils.LOKI_PREFIX+"/api/v1/index/stats", hs.Recovery(lokiIndexStatsHandler()))
	hs.Router.GET(server_utils.LOKI_PREFIX+"/api/v1/series", hs.Recovery(lokiSeriesHandler()))
	hs.Router.POST(server_utils.LOKI_PREFIX+"/api/v1/series", hs.Recovery(lokiSeriesHandler()))

	//splunk endpoint
	hs.Router.GET("/services/collector/health", hs.Recovery(getHealthHandler()))
	hs.Router.GET("/services/collector/health/1.0", hs.Recovery(getHealthHandler()))

	//OTSDB query endpoint
	hs.Router.GET(server_utils.OTSDB_PREFIX+"/api/query", hs.Recovery(otsdbMetricQueryHandler()))
	hs.Router.POST(server_utils.OTSDB_PREFIX+"/api/query", hs.Recovery(otsdbMetricQueryHandler()))
	hs.Router.POST(server_utils.OTSDB_PREFIX+"/api/v1/query/exp", hs.Recovery(otsdbMetricQueryExpHandler()))

	//prometheus query endpoint
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/v1/query", hs.Recovery(metricsSearchHandler()))
	hs.Router.GET(server_utils.PROMQL_PREFIX+"/api/v1/query", hs.Recovery(metricsSearchHandler()))
	hs.Router.POST(server_utils.PROMQL_PREFIX+"/api/ui/query", hs.Recovery(uiMetricsSearchHandler()))

	// search api Handlers
	hs.Router.POST(server_utils.API_PREFIX+"/echo", tracing.TraceMiddleware(hs.Recovery(pipeSearchHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/listIndices", tracing.TraceMiddleware(hs.Recovery(listIndicesHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/clusterStats", tracing.TraceMiddleware(hs.Recovery(getClusterStatsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/clusterIngestStats", tracing.TraceMiddleware(hs.Recovery(getClusterIngestStatsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/usersavedqueries/save", tracing.TraceMiddleware(hs.Recovery(saveUserSavedQueriesHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/usersavedqueries/getall", tracing.TraceMiddleware(hs.Recovery(getUserSavedQueriesAllHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/usersavedqueries/deleteone/{qname}", tracing.TraceMiddleware(hs.Recovery(deleteUserSavedQueryHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/usersavedqueries/{qname}", tracing.TraceMiddleware(hs.Recovery(SearchUserSavedQueryHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs/clear", tracing.TraceMiddleware(hs.Recovery(postPqsClearHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs/get", tracing.TraceMiddleware(hs.Recovery(getPqsEnabledHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/pqs/aggs", tracing.TraceMiddleware(hs.Recovery(postPqsAggColsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/pqs/update", tracing.TraceMiddleware(hs.Recovery(postPqsHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs", tracing.TraceMiddleware(hs.Recovery(getPqsHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/pqs/{pqid}", tracing.TraceMiddleware(hs.Recovery(getPqsByIdHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/dashboards/create", tracing.TraceMiddleware(hs.Recovery(createDashboardHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/defaultlistall", tracing.TraceMiddleware(hs.Recovery(getDefaultDashboardIdsHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/listall", tracing.TraceMiddleware(hs.Recovery(getDashboardIdsHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/dashboards/update", tracing.TraceMiddleware(hs.Recovery(updateDashboardHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/{dashboard-id}", tracing.TraceMiddleware(hs.Recovery(getDashboardIdHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/delete/{dashboard-id}", tracing.TraceMiddleware(hs.Recovery(deleteDashboardHandler())))
	hs.Router.PUT(server_utils.API_PREFIX+"/dashboards/favorite/{dashboard-id}", tracing.TraceMiddleware(hs.Recovery(favoriteDashboardHandler())))
	hs.Router.GET(server_utils.API_PREFIX+"/dashboards/listfavorites", tracing.TraceMiddleware(hs.Recovery(getFavoriteDashboardIdsHandler())))
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

	hs.Router.GET(server_utils.API_PREFIX+"/minionsearch/allMinionSearches", hs.Recovery(getAllMinionSearchesHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/minionsearch/createMinionSearches", hs.Recovery(createMinionSearchHandler()))
	hs.Router.GET(server_utils.API_PREFIX+"/minionsearch/{alertID}", hs.Recovery(getMinionSearchHandler()))

	// tracing api endpoints
	hs.Router.POST(server_utils.API_PREFIX+"/traces/search", tracing.TraceMiddleware(hs.Recovery(searchTracesHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/dependencies", tracing.TraceMiddleware(hs.Recovery(getDependencyGraphHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/generate-dep-graph", hs.Recovery(generateDependencyGraphHandler()))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/ganttChart", tracing.TraceMiddleware(hs.Recovery(ganttChartHandler())))
	hs.Router.POST(server_utils.API_PREFIX+"/traces/count", tracing.TraceMiddleware(hs.Recovery((totalTracesHandler()))))
	// query server should still setup ES APIs for Kibana integration
	hs.Router.POST(server_utils.ELASTIC_PREFIX+"/_bulk", hs.Recovery(esPostBulkHandler()))
	hs.Router.PUT(server_utils.ELASTIC_PREFIX+"/{indexName}", hs.Recovery(esPutIndexHandler()))

	hs.Router.GET(server_utils.API_PREFIX+"/system-info", tracing.TraceMiddleware(hs.Recovery(getSystemInfoHandler())))
	if config.IsDebugMode() {
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

				fasthttp.ServeFile(ctx, "static/"+filepath)
			})
		}

		hook(hs.Router, htmlTemplate)
	}

	hs.ln, err = net.Listen("tcp4", hs.Addr)
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
	var g run.Group

	if config.IsTlsEnabled() {
		cfg := &tls.Config{
			Certificates: make([]tls.Certificate, 1),
		}

		cfg.Certificates[0], err = tls.LoadX509KeyPair(config.GetTLSCertificatePath(), config.GetTLSPrivateKeyPath())

		if err != nil {
			fmt.Println("Run: error in loading TLS certificate: ", err)
			log.Fatalf("Run: error in loading TLS certificate: %v", err)
		}

		hs.lnTls = tls.NewListener(hs.ln, cfg)

		// run fasthttp server
		g.Add(func() error {
			return s.Serve(hs.lnTls)
		}, func(e error) {
			_ = hs.ln.Close()
		})

	} else {
		// run fasthttp server
		g.Add(func() error {
			return s.Serve(hs.ln)
		}, func(e error) {
			_ = hs.ln.Close()
		})
	}
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
	hs.ln, err = net.Listen("tcp4", hs.Addr)
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
