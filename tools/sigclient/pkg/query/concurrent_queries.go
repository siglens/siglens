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

package query

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/fasthttp/websocket"
	log "github.com/sirupsen/logrus"
)

var qid = int64(1)
var numConnResets = int64(0)
var numTimeouts = int64(0)
var numFailed = int64(0)
var numCompleted = int64(0)

func sendQuery(queryReq map[string]interface{}, qid int64, websocketURL string) {

	conn, _, err := websocket.DefaultDialer.Dial(websocketURL, nil)
	if err != nil {
		log.Errorf("sendQuery: Error connecting to the websocket: %v\n", err)
		atomic.AddInt64(&numConnResets, 1)
		return
	}
	defer conn.Close()

	err = conn.WriteJSON(queryReq)
	if err != nil {
		log.Errorf("sendQuery: Received error from server, qid: %v, err: %+v\n", qid, err)
		return
	}

	readEvent := make(map[string]interface{})
	for {
		err = conn.ReadJSON(&readEvent)
		if err != nil {
			log.Infof("sendQuery: Error reading json: %v", err)
			break
		}
		switch readEvent["state"] {
		case "RUNNING":
		case "QUERY_UPDATE":
		case "TIMEOUT":
			log.Debugf("qid=%v Query timed out\n", qid)
			atomic.AddInt64(&numTimeouts, 1)
			return
		case "COMPLETE":
			log.Debugf("qid=%v Query completed\n", qid)
			atomic.AddInt64(&numCompleted, 1)
			return
		case "error":
			log.Debugf("qid=%v Query failed: %v\n", qid, readEvent)
			atomic.AddInt64(&numFailed, 1)
			return
		default:
			return
		}
	}
}

func RunConcurrentQueries(dest string, query string, numOfConcurrentQueries int, iterations int) {

	startEpoch := "now-90d"
	endEpoch := "now"
	queryLanguage := "Splunk QL"

	queryReq := map[string]interface{}{
		"state":         "query",
		"searchText":    query,
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	webSocketURL := fmt.Sprintf("ws://%v/api/search/ws", dest)

	log.Infoln()

	var wg sync.WaitGroup
	for itr := 0; itr < iterations; itr++ {
		log.Infof("Iteration: %v\n", itr+1)
		numConnResets = 0
		numTimeouts = 0
		numFailed = 0
		numCompleted = 0
		for i := 0; i < numOfConcurrentQueries; i++ {
			wg.Add(1)
			go func(qid int64) {
				sendQuery(queryReq, qid, webSocketURL)
				wg.Done()
			}(qid)
			qid++
		}
		log.Info("Waiting for all queries to complete\n")
		wg.Wait()
		log.Infof("Conn resets: %v\n", int(numConnResets))
		log.Infof("Completed: %v\n", int(numCompleted))
		log.Infof("Failed: %v\n", int(numFailed))
		log.Infof("Timeouts: %v\n", int(numTimeouts))
		log.Info("------------------------------------------------------------------------------\n")
	}
}
