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
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/utils"

	"github.com/fasthttp/websocket"

	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/dashboards"
	esreader "github.com/siglens/siglens/pkg/es/reader"
	esutils "github.com/siglens/siglens/pkg/es/utils"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/health"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/integrations/loki"
	otsdbquery "github.com/siglens/siglens/pkg/integrations/otsdb/query"
	prom "github.com/siglens/siglens/pkg/integrations/prometheus/promql"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/sampledataset"
	tracinghandler "github.com/siglens/siglens/pkg/segment/tracing/handler"
	usq "github.com/siglens/siglens/pkg/usersavedqueries"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func getHealthHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessGetHealth(ctx)
	}
}

func esGetSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		instrumentation.IncrementInt64Counter(instrumentation.QUERY_COUNT, 1)
		esreader.ProcessSearchRequest(ctx, 0)
	}
}

func listIndicesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		pipesearch.ListIndicesHandler(ctx, 0)
	}
}

func otsdbMetricQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		otsdbquery.MetricsQueryParser(ctx, 0)
	}
}

func otsdbMetricQueryExpHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		otsdbquery.MetricsQueryExpressionsParser(ctx)
	}
}

func metricsSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		prom.ProcessMetricsSearchRequest(ctx, 0)
	}
}
func uiMetricsSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		prom.ProcessUiMetricsSearchRequest(ctx, 0)
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
		eswriter.ProcessBulkRequest(ctx, 0, processKibanaIngestRequest)
	}
}

func esPutIndexHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessPutIndex(ctx, 0)
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
		esreader.ProcessSingleDocGetRequest(ctx, 0)

	}
}

func esGetIndexAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessGetIndexAlias(ctx, 0)
	}
}

func esGetAliasHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessGetAlias(ctx, 0)
	}
}

func esPostAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessPostAliasesRequest(ctx, 0)
	}
}

func esPutIndexAliasHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessPutAliasesRequest(ctx, 0)
	}
}

func esGetAllAliasesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessGetAllAliases(ctx, 0)
	}
}

func esGetIndexAliasExistsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		eswriter.ProcessIndexAliasExist(ctx, 0)
	}
}

func pipeSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		pipesearch.ProcessPipeSearchRequest(ctx, 0)
	}
}

func dashboardPipeSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		pipesearch.ProcessPipeSearchRequest(ctx, 0)
	}
}

var upgrader = websocket.FastHTTPUpgrader{
	// todo if we don't check origin in this func, then cross-site forgery could happen
	CheckOrigin:     func(r *fasthttp.RequestCtx) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

func pipeSearchWebsocketHandler(myid uint64) func(ctx *fasthttp.RequestCtx) {

	return func(ctx *fasthttp.RequestCtx) {
		startTime := time.Now()
		err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
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
			pipesearch.ProcessPipeSearchWebsocket(conn, myid)
		})
		if err != nil {
			log.Errorf("PipeSearchWebsocketHandler: Error upgrading websocket connection %+v", err)
			return
		}

		// Logging data to access.log
		// timeStamp <logged-in user> <request URI> <request body> <response status code> <elapsed time in ms>
		duration := time.Since(startTime).Milliseconds()
		utils.AddAccessLogEntry(dtypeutils.AccessLogData{
			TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
			UserName:    "No-User", // TODO : Add logged in user when user auth is implemented
			URI:         ctx.Request.URI().String(),
			RequestBody: string(ctx.PostBody()),
			StatusCode:  ctx.Response.StatusCode(),
			Duration:    duration,
		}, "access.log")
	}
}

func getClusterStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessClusterStatsHandler(ctx, 0)
	}
}

func getClusterIngestStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		health.ProcessClusterIngestStatsHandler(ctx, 0)
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

func createDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessCreateDashboardRequest(ctx, 0)
	}
}

func getDashboardIdsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessListAllRequest(ctx, 0)
	}
}

func getDefaultDashboardIdsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessListAllDefaultDBRequest(ctx, 0)
	}
}

func updateDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessUpdateDashboardRequest(ctx, 0)
	}
}

func getDashboardIdHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessGetDashboardRequest(ctx)
	}
}

func deleteDashboardHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		dashboards.ProcessDeleteDashboardRequest(ctx, 0)
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
		sampledataset.ProcessSyntheicDataRequest(ctx, 0)
	}
}

func lokiLabelsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		loki.ProcessLokiLabelRequest(ctx)
	}
}

func lokiLabelValueHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		loki.ProcessLokiLabelValuesRequest(ctx, 0)
	}
}

func lokiQueryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		loki.ProcessQueryRequest(ctx, 0)
	}
}

func lokiIndexStatsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		loki.ProcessIndexStatsRequest(ctx, 0)
	}
}

func lokiSeriesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		loki.ProcessLokiSeriesRequest(ctx, 0)
	}
}

// alerting apis
func createAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessCreateAlertRequest(ctx)
	}
}

func getAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetAlertRequest(ctx)
	}
}

func getAllAlertsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetAllAlertsRequest(ctx)
	}
}

func getAllMinionSearchesHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetAllMinionSearchesRequest(ctx)
	}
}

func updateAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessUpdateAlertRequest(ctx)
	}
}

func deleteAlertHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessDeleteAlertRequest(ctx)
	}
}

func createContactHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessCreateContactRequest(ctx)
	}
}

func getAllContactsHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetAllContactsRequest(ctx)
	}
}

func updateContactHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessUpdateContactRequest(ctx)
	}
}

func createNotificationHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessCreateNotificationRequest(ctx)
	}
}

func deleteContactHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessDeleteContactRequest(ctx)
	}
}

func createMinionSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessCreateLogMinionSearchRequest(ctx)
	}
}

func getMinionSearchHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		alertsHandler.ProcessGetMinionSearchRequest(ctx)
	}
}

func liveTailHandler(myid uint64) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
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
			pipesearch.ProcessPipeSearchWebsocket(conn, myid)
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
		tracinghandler.ProcessSearchTracesRequest(ctx, 0)
	}
}
func getDependencyGraphHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		tracinghandler.ProcessDependencyRequest(ctx, 0)
	}
}
