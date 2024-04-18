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
	"time"

	"github.com/fasthttp/websocket"

	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/cfghandler"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/dashboards"
	esreader "github.com/siglens/siglens/pkg/es/reader"
	esutils "github.com/siglens/siglens/pkg/es/utils"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/health"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/integrations/loki"
	otsdbquery "github.com/siglens/siglens/pkg/integrations/otsdb/query"
	prom "github.com/siglens/siglens/pkg/integrations/prometheus/promql"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/sampledataset"
	tracinghandler "github.com/siglens/siglens/pkg/segment/tracing/handler"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	systemconfig "github.com/siglens/siglens/pkg/systemConfig"
	usq "github.com/siglens/siglens/pkg/usersavedqueries"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type VersionResponse struct {
	Version string `json:"version"`
}

func getVersionHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessVersionInfo(ctx)
	}
}

func getHealthHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessGetHealth(ctx)
	}
}

func esGetSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.QUERY_COUNT, 1)
		serverutils.CallWithOrgIdQuery(esreader.ProcessSearchRequest, ctx)
	}
}

func listIndicesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(pipesearch.ListIndicesHandler, ctx)
	}
}

func otsdbMetricQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(otsdbquery.MetricsQueryParser, ctx)
	}
}

func otsdbMetricQueryExpHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		otsdbquery.MetricsQueryExpressionsParser(ctx)
	}
}

func metricsSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(prom.ProcessMetricsSearchRequest, ctx)
	}
}
func uiMetricsSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(prom.ProcessUiMetricsSearchRequest, ctx)
	}
}

func esGreetHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		esutils.ProcessGreetHandler(ctx)
	}
}

func processKibanaIngestRequest(ctx *fasthttp.RequestCtx, request map[string]interface{},
	indexNameConverted string, updateArg bool, idVal string, tsNow uint64, myid uint64) error {
	return nil
}

func esPostBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)

		handler := func(ctx *fasthttp.RequestCtx, orgId uint64) {
			eswriter.ProcessBulkRequest(ctx, orgId, processKibanaIngestRequest)
		}

		serverutils.CallWithOrgId(handler, ctx)
	}
}

func esPutIndexHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgId(eswriter.ProcessPutIndex, ctx)
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

func headHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
}

func esGetSingleDocHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {

		instrumentation.IncrementInt64Counter(instrumentation.QUERY_COUNT, 1)
		serverutils.CallWithOrgId(esreader.ProcessSingleDocGetRequest, ctx)

	}
}

func esGetIndexAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgId(eswriter.ProcessGetIndexAlias, ctx)
	}
}

func esGetAliasHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(eswriter.ProcessGetAlias, ctx)
	}
}

func esPostAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(eswriter.ProcessPostAliasesRequest, ctx)
	}
}

func esPutIndexAliasHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(eswriter.ProcessPutAliasesRequest, ctx)
	}
}

func esGetAllAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(eswriter.ProcessGetAllAliases, ctx)
	}
}

func esGetIndexAliasExistsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(eswriter.ProcessIndexAliasExist, ctx)
	}
}

func pipeSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(pipesearch.ProcessPipeSearchRequest, ctx)
	}
}

func dashboardPipeSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(pipesearch.ProcessPipeSearchRequest, ctx)
	}
}

var upgrader = websocket.FastHTTPUpgrader{
	// todo if we don't check origin in this func, then cross-site forgery could happen
	CheckOrigin:     func(r *fasthttp.RequestCtx) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

func pipeSearchWebsocketHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
			var orgId uint64
			var err error
			if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
				orgId, err = hook(ctx)
				if err != nil {
					log.Errorf("pipeSearchWebsocketHandler: failed to extract orgId from context. Err=%+v", err)
					utils.SetBadMsg(ctx, "")
					return
				}
			}
			defer func() {
				deadline := time.Now().Add(time.Second * 5)
				err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), deadline)
				if err != nil {
					if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
						log.Errorf("PipeSearchWebsocketHandler: failed to write close control message: %v", err)
					}
					return
				}
				err = conn.Close()
				if err != nil {
					if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
						log.Errorf("PipeSearchWebsocketHandler: unexpected error when trying to close websocket connection: %v", err)
					}
					return
				}
			}()
			pipesearch.ProcessPipeSearchWebsocket(conn, orgId, ctx)
		})
		if err != nil {
			log.Errorf("PipeSearchWebsocketHandler: Error upgrading websocket connection %+v", err)
			return
		}
	}
}

func getClusterStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if hook := hooks.GlobalHooks.StatsHandlerHook; hook != nil {
			hook(ctx, 0)
		} else {
			serverutils.CallWithOrgIdQuery(health.ProcessClusterStatsHandler, ctx)
		}
	}
}

func getClusterIngestStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if hook := hooks.GlobalHooks.IngestStatsHandlerHook; hook != nil {
			hook(ctx, 0)
		} else {
			serverutils.CallWithOrgIdQuery(health.ProcessClusterIngestStatsHandler, ctx)
		}
	}
}

func saveUserSavedQueriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		usq.SaveUserQueries(ctx)
	}
}

func getUserSavedQueriesAllHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		usq.GetUserSavedQueriesAll(ctx)
	}
}

func deleteUserSavedQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		usq.DeleteUserSavedQuery(ctx)
	}
}

func SearchUserSavedQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		usq.SearchUserSavedQuery(ctx)
	}
}

func postPqsClearHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		querytracker.PostPqsClear(ctx)
	}
}

func postPqsAggColsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		querytracker.PostPqsAggCols(ctx)
	}
}

func getPqsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		querytracker.GetPQSSummary(ctx)
	}
}

func getPqsByIdHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		querytracker.GetPQSById(ctx)
	}
}

func postPqsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		cfghandler.PostPqsUpdate(ctx)
	}
}

func getPqsEnabledHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		cfghandler.GetPqsEnabled(ctx)
	}
}

func createDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(dashboards.ProcessCreateDashboardRequest, ctx)
	}
}

func favoriteDashboardHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessFavoriteRequest(ctx)
	}
}

func getFavoriteDashboardIdsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(dashboards.ProcessListFavoritesRequest, ctx)
	}
}
func getDashboardIdsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(dashboards.ProcessListAllRequest, ctx)
	}
}

func getDefaultDashboardIdsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(dashboards.ProcessListAllDefaultDBRequest, ctx)
	}
}

func updateDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(dashboards.ProcessUpdateDashboardRequest, ctx)
	}
}

func getDashboardIdHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessGetDashboardRequest(ctx)
	}
}

func deleteDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(dashboards.ProcessDeleteDashboardRequest, ctx)
	}
}

func getSafeHealthHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessSafeHealth(ctx)
	}
}

func sampleDatasetBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)
		serverutils.CallWithOrgId(sampledataset.ProcessSyntheicDataRequest, ctx)
	}
}

func lokiLabelsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		loki.ProcessLokiLabelRequest(ctx)
	}
}

func lokiLabelValueHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(loki.ProcessLokiLabelValuesRequest, ctx)
	}
}

func lokiQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(loki.ProcessQueryRequest, ctx)
	}
}

func lokiIndexStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(loki.ProcessIndexStatsRequest, ctx)
	}
}

func lokiSeriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(loki.ProcessLokiSeriesRequest, ctx)
	}
}

// alerting apis
func createAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(alertsHandler.ProcessCreateAlertRequest, ctx)
	}
}

func silenceAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessSilenceAlertRequest(ctx)
	}
}

func getAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetAlertRequest(ctx)
	}
}

func getAllAlertsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(alertsHandler.ProcessGetAllAlertsRequest, ctx)
	}
}

func getAllMinionSearchesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(alertsHandler.ProcessGetAllMinionSearchesRequest, ctx)
	}
}

func updateAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessUpdateAlertRequest(ctx)
	}
}

func alertHistoryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessAlertHistoryRequest(ctx)
	}
}

func deleteAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessDeleteAlertRequest(ctx)
	}
}

func createContactHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(alertsHandler.ProcessCreateContactRequest, ctx)
	}
}

func getAllContactsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(alertsHandler.ProcessGetAllContactsRequest, ctx)
	}
}

func updateContactHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessUpdateContactRequest(ctx)
	}
}

func deleteContactHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessDeleteContactRequest(ctx)
	}
}

func createMinionSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(alertsHandler.ProcessCreateLogMinionSearchRequest, ctx)
	}
}

func getMinionSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetMinionSearchRequest(ctx)
	}
}

func liveTailHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
			var orgId uint64
			var err error
			if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
				orgId, err = hook(ctx)
				if err != nil {
					log.Errorf("ProcessClusterIngestStatsHandler: failed to extract orgId from context. Err=%+v", err)
					utils.SetBadMsg(ctx, "")
					return
				}
			}

			defer func() {
				deadline := time.Now().Add(time.Second * 5)
				err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), deadline)
				if err != nil {
					if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
						log.Errorf("liveTailHandler: failed to write close control message: %v", err)
					}
					return
				}
				err = conn.Close()
				if err != nil {
					if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
						log.Errorf("liveTailHandler: unexpected error when trying to close websocket connection: %v", err)
					}
					return
				}
			}()
			pipesearch.ProcessPipeSearchWebsocket(conn, orgId, ctx)
		})
		if err != nil {
			log.Errorf("liveTailHandler: Error upgrading websocket connection %+v", err)
			return
		}
	}
}

// Tracing apis
func searchTracesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(tracinghandler.ProcessSearchTracesRequest, ctx)
	}
}

func totalTracesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(tracinghandler.ProcessTotalTracesRequest, ctx)
	}
}

func getDependencyGraphHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(tracinghandler.ProcessAggregatedDependencyGraphs, ctx)
	}
}

func generateDependencyGraphHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(tracinghandler.ProcessGeneratedDepGraph, ctx)
	}
}

func ganttChartHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithOrgIdQuery(tracinghandler.ProcessGanttChartRequest, ctx)
	}
}

func getSystemInfoHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		systemconfig.GetSystemInfo(ctx)
	}
}
