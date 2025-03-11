package alertsHandler

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

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/integrations/prometheus/promql"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

var s = gocron.NewScheduler(time.UTC)

func VerifyAlertCronJobExists(alertDataObj *alertutils.AlertDetails) bool {
	job_ids := s.GetAllTags()
	for _, id := range job_ids {
		if alertDataObj.AlertId == id {
			return true
		}
	}
	return false
}

func AddCronJob(alertDataObj *alertutils.AlertDetails) (*gocron.Job, error) {
	evaluationIntervalInSec := int(alertDataObj.EvalInterval * 60)

	var evaluateFunc interface{}

	if alertDataObj.AlertType == alertutils.AlertTypeLogs {
		evaluateFunc = evaluateLogAlert
	} else if alertDataObj.AlertType == alertutils.AlertTypeMetrics {
		evaluateFunc = evaluateMetricsAlert
	} else {
		log.Errorf("AddCronJob: AlertType=%v is not Logs or Metrics. Alert=%+v", alertDataObj.AlertType, alertDataObj.AlertName)
		return nil, fmt.Errorf("AlertType is not Logs or Metrics. Alert=%+v", alertDataObj.AlertName)
	}

	cron_job, err := s.Every(evaluationIntervalInSec).Second().Tag(alertDataObj.AlertId).DoWithJobDetails(evaluateFunc, alertDataObj)
	if err != nil {
		log.Errorf("AddCronJob: Error adding a new cronJob to the CRON Scheduler: %s", err)
		return &gocron.Job{}, err
	}
	s.StartAsync()
	// TODO: for multinode set up, set create and set node_id to which this alert will be assigned
	// TODO: node_id should be created using hash function
	return cron_job, nil
}

func AddMinionSearchCronJob(alertDataObj *alertutils.MinionSearch) (*gocron.Job, error) {
	evaluationIntervalInSec := int(alertDataObj.EvalInterval * 60)

	cron_job, err := s.Every(evaluationIntervalInSec).Second().Tag(alertDataObj.AlertId).DoWithJobDetails(evaluateMinionSearch, alertDataObj)
	if err != nil {
		log.Errorf("AddMinionSearchCronJob: Error adding a new cronJob to the CRON Scheduler: %s", err)
		return &gocron.Job{}, err
	}
	s.StartAsync()
	// TODO: for multinode set up, set create and set node_id to which this alert will be assigned
	// TODO: node_id should be created using hash function
	return cron_job, nil
}

func RemoveCronJob(alertId string) error {
	err := s.RemoveByTag(alertId)
	if err != nil {
		log.Errorf("ALERTSERVICE: RemoveCronJob error %v.", err)
		if strings.Contains(err.Error(), "no jobs found") {
			return nil
		}
		return err
	}
	return nil
}

func updateAlertStateAndCreateAlertHistory(alertDetails *alertutils.AlertDetails, alertState alertutils.AlertState, eventDesc string, alertNotificationSent bool) error {
	err := updateAlertState(alertDetails.AlertId, alertState, alertNotificationSent)
	if err != nil {
		log.Errorf("ALERTSERVICE: updateAlertStateAndCreateAlertHistory: could not update the state to %v. Alert=%+v & err=%+v.", alertState, alertDetails.AlertName, err)
		return err
	}

	alertEvent := alertutils.AlertHistoryDetails{
		AlertId:          alertDetails.AlertId,
		AlertType:        alertDetails.AlertType,
		AlertState:       alertState,
		EventDescription: eventDesc,
		UserName:         alertutils.SystemGeneratedAlert,
		EventTriggeredAt: time.Now().UTC(),
	}
	_, err = databaseObj.CreateAlertHistory(&alertEvent)
	if err != nil {
		log.Errorf("ALERTSERVICE: updateAlertStateAndCreateAlertHistory: could not create alert event in alert history. found error = %v", err)
		return err
	}
	return nil
}

// An Alert State can be updated to Firing, when the previous (IntervalCount - 1) Evaluations + Current Evaluation satisfies the conditions.
// Meaning, the (IntervalCount - 1) evaluations and the current Evaluations State should be in either Pending or Firing.
// The (IntervalCount - 1) evaluations will be Fetched from the AlertHistory Table.
func shouldUpdateAlertStateToFiring(alertDetails *alertutils.AlertDetails, currentState alertutils.AlertState) bool {
	if !alertutils.IsAlertStatePendingOrFiring(currentState) {
		return false
	}

	intervalCount := alertDetails.EvalWindow / alertDetails.EvalInterval
	if intervalCount == 0 {
		log.Errorf("ALERTSERVICE: shouldUpdateAlertStateToFiring: EvalWindow=%v is less than EvalInterval=%v. Alert=%+v", alertDetails.EvalWindow, alertDetails.EvalInterval, alertDetails.AlertName)
		return false
	}

	if intervalCount == 1 {
		return true
	}

	alertHistoryList, err := databaseObj.GetAlertHistoryByAlertID(&alertutils.AlertHistoryQueryParams{
		AlertId:   alertDetails.AlertId,
		Limit:     intervalCount - 1,
		SortOrder: alertutils.DESC,
	})
	if err != nil {
		log.Errorf("ALERTSERVICE: shouldUpdateAlertStateToFiring: Error getting AlertHistory. Alert=%+v & err=%+v.", alertDetails.AlertName, err)
		return false
	}

	if len(alertHistoryList) < int(intervalCount-1) {
		return false
	}

	for _, alertHistory := range alertHistoryList {
		if !alertutils.IsAlertStatePendingOrFiring(alertHistory.AlertState) {
			return false
		}
	}

	return true
}

func GetLatestAlertHistory(alertId string) (*alertutils.AlertHistoryDetails, error) {
	alertHistoryList, err := databaseObj.GetAlertHistoryByAlertID(&alertutils.AlertHistoryQueryParams{
		AlertId:   alertId,
		Limit:     1,
		SortOrder: alertutils.DESC,
	})
	if err != nil {
		log.Errorf("ALERTSERVICE: GetLatestAlertHistory: Error getting AlertHistory. AlertId=%v & err=%+v.", alertId, err)
		return nil, err
	}

	if len(alertHistoryList) == 0 {
		return nil, nil
	}

	return alertHistoryList[0], nil
}

func handleAlertCondition(alertToEvaluate *alertutils.AlertDetails, isAlertConditionMatched bool, alertDataMessage string) error {
	var newAlertState alertutils.AlertState
	var eventDesc string
	var alertNotificationSent bool
	var err error

	if isAlertConditionMatched {
		newAlertState = alertutils.Pending
		eventDesc = alertutils.AlertPending

		if shouldUpdateAlertStateToFiring(alertToEvaluate, newAlertState) {
			newAlertState = alertutils.Firing
			eventDesc = alertutils.AlertFiring
		}

		// If the Alert State is updated to Firing, then we should send the Alert Notification.
		// If the previous state was Firing, then the cooldown period on the Notification Handler will decide if the notification should be sent.
		if newAlertState == alertutils.Firing {
			alertNotificationSent, err = NotifyAlertHandlerRequest(alertToEvaluate.AlertId, newAlertState, alertDataMessage)
			if err != nil {
				log.Errorf("handleAlertCondition: Could not send Alert Notification. found error = %v", err)
			}
		}
	} else {
		newAlertState = alertutils.Normal
		eventDesc = alertutils.AlertNormal

		// The Alert state is Normal, then we should send the Alert Notification.
		// The cooldown period on the Notification Handler will decide if the notification should be sent. So that false positives are avoided.
		// The Notification handler is expected to send the Normal State Notification, only if the previous Notification sent was Firing.
		alertNotificationSent, err = NotifyAlertHandlerRequest(alertToEvaluate.AlertId, alertutils.Normal, "The Alert State has been updated to Normal.")
		if err != nil {
			log.Errorf("handleAlertCondition: Could not send Alert Notification. found error = %v", err)
		}
	}

	err = updateAlertStateAndCreateAlertHistory(alertToEvaluate, newAlertState, eventDesc, alertNotificationSent)
	if err != nil {
		log.Errorf("ALERTSERVICE: handleAlertCondition: Error in updateAlertStateAndCreateAlertHistory. AlertState=%v, Alert=%+v & err=%+v", newAlertState, alertToEvaluate.AlertName, err)
	}

	return err
}

func getLogsQueryLinkForTheAlert(alertDetails *alertutils.AlertDetails, timeRange *dtypeutils.TimeRange) string {
	if timeRange == nil {
		log.Errorf("ALERTSERVICE: getLogsQueryLinkForTheAlert: TimeRange is nil. Alert=%+v", alertDetails.AlertName)
		return ""
	}

	if alertDetails == nil {
		log.Errorf("ALERTSERVICE: getLogsQueryLinkForTheAlert: AlertDetails is nil.")
		return ""
	}

	baseURL := config.GetQueryServerBaseUrl() + "/index.html"

	// query parameters
	params := url.Values{}
	params.Add("searchText", alertDetails.QueryParams.QueryText)
	params.Add("startEpoch", fmt.Sprintf("%v", timeRange.StartEpochMs))
	params.Add("endEpoch", fmt.Sprintf("%v", timeRange.EndEpochMs))
	params.Add("indexName", alertDetails.QueryParams.Index)
	params.Add("queryLanguage", alertDetails.QueryParams.QueryLanguage)

	// Construct the full URL
	fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

	return fullURL
}

func getMetricsQueryLinkForTheAlert(alertDetails *alertutils.AlertDetails, parsedJsonMap map[string]interface{}) string {
	if alertDetails == nil {
		log.Errorf("ALERTSERVICE: getMetricsQueryLinkForTheAlert: AlertDetails is nil.")
		return ""
	}

	baseURL := config.GetQueryServerBaseUrl() + "/metrics-explorer.html"

	params := url.Values{}

	jsonData, err := json.Marshal(parsedJsonMap)
	if err != nil {
		log.Errorf("ALERTSERVICE: getMetricsQueryLinkForTheAlert: Error marshalling parsedJsonMap=%v. Alert=%+v, err=%+v", parsedJsonMap, alertDetails.AlertName, err)
		return ""
	}

	params.Add("queryString", string(jsonData))

	fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

	return fullURL
}

func evaluateLogAlert(alertToEvaluate *alertutils.AlertDetails, job gocron.Job) {
	searchResponse, timeRange, err := pipesearch.ProcessAlertsPipeSearchRequest(alertToEvaluate.QueryParams, alertToEvaluate.OrgId, nil) // TODO: CronJob should really record the user who created it. This user should then be set in the context and passed along.
	if err != nil {
		log.Errorf("ALERTSERVICE: evaluateLogAlert: Error processing logs query. Alert=%+v, err=%+v", alertToEvaluate.AlertName, err)
		return
	}

	isAlertConditionMatched, err := evaluateLogsQueryConditions(searchResponse, &alertToEvaluate.Condition, alertToEvaluate.Value)
	if err != nil {
		log.Errorf("ALERTSERVICE: evaluateLogAlert: Error evaluating logs query conditions. Alert=%+v, err=%+v", alertToEvaluate.AlertName, err)
		return
	}

	alertDataMessage := getLogsQueryLinkForTheAlert(alertToEvaluate, timeRange)

	err = handleAlertCondition(alertToEvaluate, isAlertConditionMatched, alertDataMessage)
	if err != nil {
		log.Errorf("ALERTSERVICE: evaluateLogAlert: Error in handleAlertCondition. Alert=%+v & err=%+v.", alertToEvaluate.AlertName, err)
	}
}

func evaluateMetricsAlert(alertToEvaluate *alertutils.AlertDetails, job gocron.Job) {
	if alertToEvaluate.AlertType != alertutils.AlertTypeMetrics {
		log.Errorf("ALERTSERVICE: evaluateMetricsAlert: AlertType is not Metrics. Alert=%+v", alertToEvaluate.AlertName)
		return
	}

	qid := rutils.GetNextQid()

	start, end, queries, formulas, errorLog, parsedJsonMap, err := promql.ParseMetricTimeSeriesRequest([]byte(alertToEvaluate.MetricsQueryParamsString))
	if err != nil {
		log.Errorf("ALERTSERVICE: evaluateMetricsAlert: Error parsing metrics query. Alert=%+v, ErrLog=%v, err=%+v", alertToEvaluate.AlertName, errorLog, err)
		return
	}

	queryRes, _, _, extraMsgToLog, err := promql.ProcessMetricsQueryRequest(queries, formulas, start, end, alertToEvaluate.OrgId, qid)
	if err != nil {
		log.Errorf("ALERTSERVICE: evaluateMetricsAlert: Error processing metrics query. Alert=%+v, ExtraMsgToLog=%v, err=%+v", alertToEvaluate.AlertName, extraMsgToLog, err)
		return
	}

	alertsDataList := evaluateMetricsQueryConditions(queryRes, &alertToEvaluate.Condition, alertToEvaluate.Value)

	isAlertConditionMatched := len(alertsDataList) > 0

	alertDataMessage := ""

	if isAlertConditionMatched {
		parsedJsonMap["start"] = start
		parsedJsonMap["end"] = end

		alertDataMessage = getMetricsQueryLinkForTheAlert(alertToEvaluate, parsedJsonMap)
	}

	err = handleAlertCondition(alertToEvaluate, isAlertConditionMatched, alertDataMessage)
	if err != nil {
		log.Errorf("ALERTSERVICE: evaluateMetricsAlert: Error in handleAlertCondition. Alert=%+v & err=%+v.", alertToEvaluate.AlertName, err)
	}
}

func updateAlertState(alertId string, alertState alertutils.AlertState, alertNotificationSent bool) error {
	err := databaseObj.UpdateAlertStateAndNotificationDetails(alertId, alertState, alertNotificationSent)
	return err
}

func evaluateConditions(serResVal float64, queryCond *alertutils.AlertQueryCondition, val float64) bool {
	switch *queryCond {
	case alertutils.IsAbove:
		return serResVal > val
	case alertutils.IsBelow:
		return serResVal < val
	case alertutils.IsEqualTo:
		return serResVal == val
	case alertutils.IsNotEqualTo:
		return serResVal != val
	case alertutils.HasNoValue:
		return serResVal == 0
	default:
		return false
	}
}

func evaluateMetricsQueryConditions(queryRes *mresults.MetricsResult, queryCond *alertutils.AlertQueryCondition, alertValue float64) []alertutils.MetricAlertData {
	alertsDataList := make([]alertutils.MetricAlertData, 0)

	for seriesId, tsMap := range queryRes.Results {
		for ts, val := range tsMap {
			toBeAlerted := evaluateConditions(val, queryCond, alertValue)
			if toBeAlerted {
				alertData := alertutils.MetricAlertData{
					SeriesId:  seriesId,
					Timestamp: ts,
					Value:     val,
				}
				alertsDataList = append(alertsDataList, alertData)
			}
		}
	}

	return alertsDataList
}

func evaluateLogsQueryConditions(searchResponse *structs.PipeSearchResponseOuter, queryCond *alertutils.AlertQueryCondition, alertValue float64) (bool, error) {
	if searchResponse == nil {
		err := fmt.Errorf("ALERTSERVICE: evaluateLogsQueryConditions: searchResponse is nil")
		log.Error(err.Error())
		return false, err
	}

	if len(searchResponse.MeasureAggregationCols) > 0 {
		return evaluateRecordsMeasureAggsAlertCondition(searchResponse, queryCond, alertValue)
	} else if searchResponse.MeasureResults != nil && len(searchResponse.MeasureResults) > 0 {
		return evaluateMeasureResultsAlertCondition(searchResponse, queryCond, alertValue)
	}

	return false, nil
}

func evaluateMeasureResultsAlertCondition(searchResponse *structs.PipeSearchResponseOuter, queryCond *alertutils.AlertQueryCondition, alertValue float64) (bool, error) {
	if searchResponse == nil {
		err := fmt.Errorf("ALERTSERVICE: evaluateMeasureResultsAlertCondition: searchResponse is nil")
		log.Error(err.Error())
		return false, err
	}

	firstBucket := searchResponse.MeasureResults[0]
	if len(firstBucket.MeasureVal) > 1 {
		err := fmt.Errorf("ALERTSERVICE: evaluateMeasureResultsAlertCondition: The Query has more than 1 Measure Column")
		log.Error(err.Error())
		return false, err
	}

	for _, bucket := range searchResponse.MeasureResults {
		for _, measureVal := range bucket.MeasureVal {
			floatVal, err := utils.ParseHumanizedValueToFloat(measureVal)
			if err != nil {
				log.Errorf("ALERTSERVICE: evaluateMeasureResultsAlertCondition: Error converting value to float. Value=%v, err=%v", measureVal, err)
				continue
			}
			toBeAlerted := evaluateConditions(floatVal, queryCond, alertValue)
			if toBeAlerted {
				return true, nil
			}
		}
	}

	return false, nil
}

func evaluateRecordsMeasureAggsAlertCondition(searchResponse *structs.PipeSearchResponseOuter, queryCond *alertutils.AlertQueryCondition, alertValue float64) (bool, error) {
	if searchResponse == nil {
		err := fmt.Errorf("ALERTSERVICE: evaluateRecordsMeasureAggsCondition: searchResponse is nil")
		log.Error(err.Error())
		return false, err
	}

	if len(searchResponse.MeasureAggregationCols) > 1 {
		err := fmt.Errorf("ALERTSERVICE: evaluateRecordsMeasureAggsCondition: The Query has more than 1 Measure Column")
		log.Error(err.Error())
		return false, err
	}

	if len(searchResponse.RenameColumns) > 0 {
		for i, measureCol := range searchResponse.MeasureAggregationCols {
			if newColName, ok := searchResponse.RenameColumns[measureCol]; ok {
				searchResponse.MeasureAggregationCols[i] = newColName
			}
		}
	}

	for _, record := range searchResponse.Hits.Hits {
		for _, col := range searchResponse.MeasureAggregationCols {
			if value, ok := record[col]; ok {
				floatVal, err := utils.ParseHumanizedValueToFloat(value)
				if err != nil {
					log.Errorf("ALERTSERVICE: evaluateRecordsMeasureAggsCondition: Error converting value to float. Value=%v, err=%v", value, err)
					continue
				}
				toBeAlerted := evaluateConditions(floatVal, queryCond, alertValue)
				if toBeAlerted {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

func updateMinionSearchStateAndCreateAlertHistory(msToEvaluate *alertutils.MinionSearch, alertState alertutils.AlertState, eventDesc string) error {
	err := updateMinionSearchState(msToEvaluate.AlertId, alertState)
	if err != nil {
		log.Errorf("MinionSearch: updateMinionSearchStateAndCreateAlertHistory: could not update the state to %v. Alert=%+v & err=%+v.", alertState, msToEvaluate.AlertName, err)
		return err
	}

	alertEvent := alertutils.AlertHistoryDetails{
		AlertId:          msToEvaluate.AlertId,
		AlertType:        alertutils.AlertTypeMinion,
		AlertState:       alertState,
		EventDescription: eventDesc,
		UserName:         alertutils.SystemGeneratedAlert,
		EventTriggeredAt: time.Now().UTC(),
	}
	_, err = databaseObj.CreateAlertHistory(&alertEvent)
	if err != nil {
		log.Errorf("MinionSearch: updateMinionSearchStateAndCreateAlertHistory: could not create alert event in alert history. found error = %v", err)
		return err
	}
	return nil
}

func evaluateMinionSearch(msToEvaluate *alertutils.MinionSearch, job gocron.Job) {
	searchResponse, _, err := pipesearch.ProcessAlertsPipeSearchRequest(msToEvaluate.QueryParams, msToEvaluate.OrgId, nil) // TODO: Cronjob should record user id of the user creating it. This user should be set in the context and sent along.
	if err != nil {
		log.Errorf("MinionSearch: evaluate: Error processing logs query. Alert=%+v, err=%+v", msToEvaluate.AlertName, err)
		return
	}

	isFiring, err := evaluateLogsQueryConditions(searchResponse, &msToEvaluate.Condition, msToEvaluate.Value)
	if err != nil {
		log.Errorf("MinionSearch: evaluate: Error evaluating logs query conditions. Alert=%+v, err=%+v", msToEvaluate.AlertName, err)
		return
	}

	if isFiring {
		err := updateMinionSearchStateAndCreateAlertHistory(msToEvaluate, alertutils.Firing, alertutils.AlertFiring)
		if err != nil {
			log.Errorf("ALERTSERVICE: evaluateMinionSearch: Error in updateMinionSearchStateAndCreateAlertHistory. AlertState=%v, Alert=%+v & err=%+v.", alertutils.Firing, msToEvaluate.AlertName, err)
		}

		_, err = NotifyAlertHandlerRequest(msToEvaluate.AlertId, alertutils.Firing, "")
		if err != nil {
			log.Errorf("MinionSearch: evaluate: Could not send Alert Notification. found error = %v", err)
			return
		}
	} else {
		err := updateMinionSearchStateAndCreateAlertHistory(msToEvaluate, alertutils.Normal, alertutils.AlertNormal)
		if err != nil {
			log.Errorf("ALERTSERVICE: evaluateMinionSearch: Error in updateMinionSearchStateAndCreateAlertHistory. AlertState=%v, Alert=%+v & err=%+v.", alertutils.Normal, msToEvaluate.AlertName, err)
		}
	}
}

func updateMinionSearchState(alertId string, alertState alertutils.AlertState) error {
	err := databaseObj.UpdateMinionSearchStateByAlertID(alertId, alertState)
	return err
}
