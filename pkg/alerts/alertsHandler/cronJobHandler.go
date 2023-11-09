package alertsHandler

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

import (
	"time"

	"github.com/go-co-op/gocron"
	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	log "github.com/sirupsen/logrus"
)

var s = gocron.NewScheduler(time.UTC)

func AddCronJob(alertDataObj *alertutils.AlertDetails) (*gocron.Job, error) {
	evaluationIntervalInSec := int(alertDataObj.EvalInterval * 60)

	cron_job, err := s.Every(evaluationIntervalInSec).Second().Tag(alertDataObj.AlertInfo.AlertId).DoWithJobDetails(evaluate, alertDataObj)
	if err != nil {
		log.Errorf("AddCronJob: Error adding a new cronJob to the CRON Scheduler: %s", err)
		return &gocron.Job{}, err
	}
	s.StartAsync()
	//TODO: for multinode set up, set create and set node_id to which this alert will be assigned
	//TODO: node_id should be created using hash function
	return cron_job, nil
}

func AddMinionSearchCronJob(alertDataObj *alertutils.MinionSearch) (*gocron.Job, error) {
	evaluationIntervalInSec := int(alertDataObj.EvalInterval * 60)

	cron_job, err := s.Every(evaluationIntervalInSec).Second().Tag(alertDataObj.AlertInfo.AlertId).DoWithJobDetails(evaluateMinionSearch, alertDataObj)
	if err != nil {
		log.Errorf("AddMinionSearchCronJob: Error adding a new cronJob to the CRON Scheduler: %s", err)
		return &gocron.Job{}, err
	}
	s.StartAsync()
	//TODO: for multinode set up, set create and set node_id to which this alert will be assigned
	//TODO: node_id should be created using hash function
	return cron_job, nil
}

func RemoveCronJob(alertId string) error {
	err := s.RemoveByTag(alertId)
	if err != nil {
		log.Errorf("ALERTSERVICE: RemoveCronJob error %v.", err)
		return err
	}
	return nil
}

func evaluate(alertToEvaluate *alertutils.AlertDetails, job gocron.Job) {
	serResVal := pipesearch.ProcessAlertsPipeSearchRequest(alertToEvaluate.QueryParams)
	if serResVal == -1 {
		log.Errorf("ALERTSERVICE: evaluate: Empty response returned by server.")
		return
	}
	isFiring := evaluateConditions(serResVal, &alertToEvaluate.Condition, alertToEvaluate.Value)
	if isFiring {
		err := updateAlertState(alertToEvaluate.AlertInfo.AlertId, alertutils.Firing)
		if err != nil {
			log.Errorf("ALERTSERVICE: evaluate: could not update the state to FIRING. Alert=%+v & err=%+v.", alertToEvaluate.AlertInfo.AlertName, err)
		}

		err = NotifyAlertHandlerRequest(alertToEvaluate.AlertInfo.AlertId)
		if err != nil {
			log.Errorf("ALERTSERVICE: evaluate: could not setup the notification handler. found error = %v", err)
			return
		}
	} else {
		err := updateAlertState(alertToEvaluate.AlertInfo.AlertId, alertutils.Inactive)
		if err != nil {
			log.Errorf("ALERTSERVICE: evaluate: could not update the state to INACTIVE. Alert=%+v & err=%+v.", alertToEvaluate.AlertInfo.AlertName, err)

		}
	}
}

func updateAlertState(alertId string, alertState alertutils.AlertState) error {
	err := databaseObj.UpdateAlertStateByAlertID(alertId, alertState)
	return err
}

func evaluateConditions(serResVal int, queryCond *alertutils.AlertQueryCondition, val float32) bool {
	switch *queryCond {
	case alertutils.IsAbove:
		return serResVal > int(val)
	case alertutils.IsBelow:
		return serResVal < int(val)
	case alertutils.IsEqualTo:
		return serResVal == int(val)
	case alertutils.IsNotEqualTo:
		return serResVal != int(val)
	case alertutils.HasNoValue:
		return serResVal == 0
	default:
		return false
	}
}

func evaluateMinionSearch(msToEvaluate *alertutils.MinionSearch, job gocron.Job) {
	serResVal := pipesearch.ProcessAlertsPipeSearchRequest(msToEvaluate.QueryParams)
	if serResVal == -1 {
		log.Errorf("MinionSearch: evaluate: Empty response returned by server.")
		return
	}
	isFiring := evaluateConditions(serResVal, &msToEvaluate.Condition, msToEvaluate.Value1)
	if isFiring {
		err := updateMinionSearchState(msToEvaluate.AlertInfo.AlertId, alertutils.Firing)
		if err != nil {
			log.Errorf("MinionSearch: evaluate: could not update the state to FIRING. Alert=%+v & err=%+v.", msToEvaluate.AlertInfo.AlertName, err)
		}

		err = NotifyAlertHandlerRequest(msToEvaluate.AlertInfo.AlertId)
		if err != nil {
			log.Errorf("MinionSearch: evaluate: could not setup the notification handler. found error = %v", err)
			return
		}
	} else {
		err := updateMinionSearchState(msToEvaluate.AlertInfo.AlertId, alertutils.Inactive)
		if err != nil {
			log.Errorf("MinionSearch: evaluate: could not update the state to INACTIVE. Alert=%+v & err=%+v.", msToEvaluate.AlertInfo.AlertName, err)

		}
	}
}

func updateMinionSearchState(alertId string, alertState alertutils.AlertState) error {
	err := databaseObj.UpdateMinionSearchStateByAlertID(alertId, alertState)
	return err
}
