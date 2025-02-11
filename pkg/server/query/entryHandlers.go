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
	"github.com/siglens/siglens/pkg/diagnostics"
	esreader "github.com/siglens/siglens/pkg/es/reader"
	esutils "github.com/siglens/siglens/pkg/es/utils"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/health"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/integrations/loki"
	otsdbquery "github.com/siglens/siglens/pkg/integrations/otsdb/query"
	prom "github.com/siglens/siglens/pkg/integrations/prometheus/promql"
	lookups "github.com/siglens/siglens/pkg/lookups"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/sampledataset"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/sortindex"
	tracinghandler "github.com/siglens/siglens/pkg/segment/tracing/handler"
	writer "github.com/siglens/siglens/pkg/segment/writer"
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
		systemconfig.ProcessVersionInfo(ctx)
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
		serverutils.CallWithMyIdQuery(esreader.ProcessSearchRequest, ctx)
	}
}

func esDeleteIndexHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(eswriter.ProcessDeleteIndex, ctx)
	}

}

func listIndicesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(pipesearch.ListIndicesHandler, ctx)
	}
}

func otsdbMetricQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(otsdbquery.MetricsQueryParser, ctx)
	}
}

func otsdbMetricQueryExpHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		otsdbquery.MetricsQueryExpressionsParser(ctx)
	}
}

func promqlMetricsInstantQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessPromqlMetricsSearchRequest, ctx)
	}
}

func promqlMetricsRangeQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessPromqlMetricsRangeSearchRequest, ctx)
	}
}

func promqlBuildInfoHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessPromqlBuildInfoRequest, ctx)
	}
}

func promqlGetLabelsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetLabelsRequest, ctx)
	}
}

func promqlGetLabelValuesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetLabelValuesRequest, ctx)
	}
}

func promqlGetSeriesByLabelHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetSeriesByLabelRequest, ctx)
	}
}
func uiMetricsSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessUiMetricsSearchRequest, ctx)
	}
}

func getAllMetricNamesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetAllMetricNamesRequest, ctx)
	}
}

func getMetricTimeSeriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetMetricTimeSeriesRequest, ctx)
	}
}

func getMetricFunctionsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetMetricFunctionsRequest, ctx)
	}
}

func getAllMetricTagsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetAllMetricTagsRequest, ctx)
	}
}

func getMetricSeriesCardinalityHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetMetricSeriesCardinalityRequest, ctx)
	}
}

func getTagKeysWithMostSeriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetTagKeysWithMostSeriesRequest, ctx)
	}
}

func getTagPairsWithMostSeriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetTagPairsWithMostSeriesRequest, ctx)
	}
}

func getTagKeysWithMostValuesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(prom.ProcessGetTagKeysWithMostValuesRequest, ctx)
	}
}

func esGreetHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		esutils.ProcessGreetHandler(ctx)
	}
}

func esPostBulkHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.POST_REQUESTS_COUNT, 1)

		handler := func(ctx *fasthttp.RequestCtx, orgId int64) {
			eswriter.ProcessBulkRequest(ctx, orgId, false)
		}

		serverutils.CallWithMyId(handler, ctx)
	}
}

func esPutIndexHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(eswriter.ProcessPutIndex, ctx)
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
		serverutils.CallWithMyId(esreader.ProcessSingleDocGetRequest, ctx)

	}
}

func esGetIndexAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyId(eswriter.ProcessGetIndexAlias, ctx)
	}
}

func esGetAliasHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(eswriter.ProcessGetAlias, ctx)
	}
}

func esPostAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(eswriter.ProcessPostAliasesRequest, ctx)
	}
}

func esPutIndexAliasHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(eswriter.ProcessPutAliasesRequest, ctx)
	}
}

func esGetAllAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(eswriter.ProcessGetAllAliases, ctx)
	}
}

func esGetIndexAliasExistsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(eswriter.ProcessIndexAliasExist, ctx)
	}
}

func pipeSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.QUERY_COUNT, 1)
		serverutils.CallWithMyIdQuery(pipesearch.ProcessPipeSearchRequest, ctx)
	}
}

func dashboardPipeSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(pipesearch.ProcessPipeSearchRequest, ctx)
	}
}

var upgrader = websocket.FastHTTPUpgrader{
	// todo if we don't check origin in this func, then cross-site forgery could happen
	CheckOrigin:     func(r *fasthttp.RequestCtx) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

func pipeSearchWebsocketHandler() func(ctx *fasthttp.RequestCtx) {
	instrumentation.IncrementInt64Counter(instrumentation.QUERY_COUNT, 1)
	return func(ctx *fasthttp.RequestCtx) {
		err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
			var orgId int64
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
			serverutils.CallWithMyIdQuery(health.ProcessClusterStatsHandler, ctx)
		}
	}
}

func getClusterIngestStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if hook := hooks.GlobalHooks.IngestStatsHandlerHook; hook != nil {
			hook(ctx, 0)
		} else {
			serverutils.CallWithMyIdQuery(health.ProcessClusterIngestStatsHandler, ctx)
		}
	}
}

func saveUserSavedQueriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(usq.SaveUserQueries, ctx)
	}
}

func getUserSavedQueriesAllHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(usq.GetUserSavedQueriesAll, ctx)
	}
}

func deleteUserSavedQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(usq.DeleteUserSavedQuery, ctx)
	}
}

func SearchUserSavedQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(usq.SearchUserSavedQuery, ctx)
	}
}

func postPqsClearHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		querytracker.PostPqsClear(ctx)
	}
}

func postPqsDeleteHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		err := writer.DeletePQSData()
		if err != nil {
			utils.SendInternalError(ctx, "Error while deleting PQS data", "", err)
			return
		}
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
		serverutils.CallWithMyIdQuery(dashboards.ProcessCreateDashboardRequest, ctx)
	}
}

func favoriteDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessFavoriteRequest, ctx)
	}
}

func updateDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessUpdateDashboardRequest, ctx)
	}
}

func getDashboardIdHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessGetDashboardRequest, ctx)
	}
}

func deleteDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessDeleteDashboardRequest, ctx)
	}
}

func listAllDashboardsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessListAllItemsRequest, ctx)
	}
}

func createFolderHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessCreateFolderRequest, ctx)
	}
}

func getFolderContentsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessGetFolderContentsRequest, ctx)
	}
}

func updateFolderHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessUpdateFolderRequest, ctx)
	}
}

func deleteFolderHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(dashboards.ProcessDeleteFolderRequest, ctx)
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
		serverutils.CallWithMyId(sampledataset.ProcessSyntheicDataRequest, ctx)
	}
}

func lokiLabelsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(loki.ProcessLokiLabelRequest, ctx)
	}
}

func lokiLabelValueHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(loki.ProcessLokiLabelValuesRequest, ctx)
	}
}

func lokiQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(loki.ProcessQueryRequest, ctx)
	}
}

func lokiIndexStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(loki.ProcessIndexStatsRequest, ctx)
	}
}

func lokiSeriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(loki.ProcessLokiSeriesRequest, ctx)
	}
}

// alerting apis
func createAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(alertsHandler.ProcessCreateAlertRequest, ctx)
	}
}

func silenceAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessSilenceAlertRequest(ctx)
	}
}

func unsilenceAlertHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessUnsilenceAlertRequest(ctx)
	}
}

func testContactPointHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessTestContactPointRequest(ctx)
	}
}

func getAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetAlertRequest(ctx)
	}
}

func getAllAlertsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(alertsHandler.ProcessGetAllAlertsRequest, ctx)
	}
}

func getAllMinionSearchesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(alertsHandler.ProcessGetAllMinionSearchesRequest, ctx)
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
		serverutils.CallWithMyIdQuery(alertsHandler.ProcessCreateContactRequest, ctx)
	}
}

func getAllContactsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(alertsHandler.ProcessGetAllContactsRequest, ctx)
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
		serverutils.CallWithMyIdQuery(alertsHandler.ProcessCreateLogMinionSearchRequest, ctx)
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
			var orgId int64
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
		serverutils.CallWithMyIdQuery(tracinghandler.ProcessSearchTracesRequest, ctx)
	}
}

func totalTracesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(tracinghandler.ProcessTotalTracesRequest, ctx)
	}
}

func getDependencyGraphHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(tracinghandler.ProcessAggregatedDependencyGraphs, ctx)
	}
}

func generateDependencyGraphHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(tracinghandler.ProcessGeneratedDepGraph, ctx)
	}
}

func ganttChartHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(tracinghandler.ProcessGanttChartRequest, ctx)
	}
}

func getSystemInfoHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		systemconfig.GetSystemInfo(ctx)
	}
}

func getInodesStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		systemconfig.GetInodeStats(ctx)
	}
}

func uploadLookupFileHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		lookups.UploadLookupFile(ctx)
	}
}

func getAllLookupFilesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		lookups.GetAllLookupFiles(ctx)
	}
}

func getLookupFileHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		lookups.GetLookupFile(ctx)
	}
}

func deleteLookupFileHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		lookups.DeleteLookupFile(ctx)
	}
}

func getQueryStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		query.GetQueryStats(ctx)
	}
}

func GetQueryTimeoutHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		cfghandler.GetQueryTimeout(ctx)
	}
}

func UpdateQueryTimeoutHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		cfghandler.UpdateQueryTimeout(ctx)
	}
}

func setSortColumnsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		sortindex.SetSortColumnsAPI(ctx)
	}
}

func collectDiagnosticsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		serverutils.CallWithMyIdQuery(diagnostics.CollectDiagnosticsAPI, ctx)
	}
}
