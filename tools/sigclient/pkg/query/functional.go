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
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Main function that tests all the queries
func FunctionalTest(dest string, dataPath string) {

	queryFiles, err := os.ReadDir(dataPath)
	if err != nil {
		log.Fatalf("FunctionalTest: Error reading directory: %v, err: %v", dataPath, err)
	}

	// validate JSON files
	for _, file := range queryFiles {
		if !strings.HasSuffix(file.Name(), ".json") {
			log.Fatalf("FunctionalTest: Invalid file format: %v. Expected .json", file.Name())
		}
	}

	// Default values
	startEpoch := "now-1h"
	endEpoch := "now"
	queryLanguage := "Splunk QL"

	// run query
	queryReq := map[string]interface{}{
		"state":         "query",
		"searchText":    "",
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	// run queries
	for idx, file := range queryFiles {
		filePath := filepath.Join(dataPath, file.Name())
		query, expRes, err := ReadAndValidateQueryFile(filePath)
		if err != nil {
			log.Fatalf("FunctionalTest: Error reading and validating query file: %v, err: %v", filePath, err)
		}

		log.Infof("FunctionalTest: qid=%v, Running query=%v", idx, query)
		queryReq["searchText"] = query
		err = EvaluateQueryForWebSocket(dest, queryReq, idx, expRes)
		if err != nil {
			log.Fatalf("FunctionalTest: Failed evaluating query via websocket, file: %v, err: %v", filePath, err)
		}
		err = EvaluateQueryForAPI(dest, queryReq, idx, expRes)
		if err != nil {
			log.Fatalf("FunctionalTest: Failed evaluating query via API, file: %v, err: %v", filePath, err)
		}

		log.Infoln()
	}

	log.Infof("FunctionalTest: All queries passed successfully")
}
