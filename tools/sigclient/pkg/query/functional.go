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
	"bufio"
	"os"
	"path"
	"path/filepath"

	"strings"

	log "github.com/sirupsen/logrus"
)

// Main function that tests all the queries
func FunctionalTest(dest string, filePath string) {

	// Check file exists
	a, err := os.Stat(filePath)
	if err != nil || a.IsDir() {
		log.Fatalf("FunctionalTest: filePath: %v is not a file, err: %v", filePath, err)
	}
	_ = a

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("FunctionalTest: Error reading filePath: %v, err: %v", filePath, err)
	}
	defer file.Close()

	baseDir, err := filepath.Abs(filepath.Dir(filePath))
	if err != nil {
		log.Fatalf("FunctionalTest: Error getting absolute path of directory of filePath: %v, err: %v", filePath, err)
	}

	scanner := bufio.NewScanner(file)

	qid := 1
	// Read line by line
	for scanner.Scan() {
		queryFilePath := strings.TrimSpace(scanner.Text())
		if queryFilePath == "" || queryFilePath[0] == '#' {
			continue
		}
		// Validate JSON Files
		if !strings.HasSuffix(queryFilePath, ".json") {
			log.Fatalf("FunctionalTest: Invalid file format: %v. Expected .json", queryFilePath)
		}

		if !path.IsAbs(queryFilePath) {
			queryFilePath = filepath.Join(baseDir, queryFilePath)
		}

		RunQuery(queryFilePath, qid, dest)
		qid++
	}

	// Check for any errors that occurred during scanning
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error while reading from filePath: %v, err: %v", filePath, err)
	}

	log.Infof("FunctionalTest: All queries passed successfully")
}

func RunQuery(filePath string, qid int, dest string) {

	// Default values
	startEpoch := "now-90d"
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
		"includeNulls":  true,
	}

	query, expRes, err := ReadAndValidateQueryFile(filePath)
	if err != nil {
		log.Fatalf("RunQuery: Error reading and validating query file: %v, err: %v", filePath, err)
	}

	log.Infof("RunQuery: qid=%v, Running file: %v, query: %v", qid, filePath, query)
	queryReq["searchText"] = query

	err = EvaluateQueryForWebSocket(dest, queryReq, qid, expRes)
	if err != nil {
		log.Fatalf("RunQuery: Failed evaluating query via websocket, file: %v, err: %v", filePath, err)
	}
	err = EvaluateQueryForAPI(dest, queryReq, qid, expRes)
	if err != nil {
		log.Fatalf("RunQuery: Failed evaluating query via API, file: %v, err: %v", filePath, err)
	}

	log.Infoln()
}
