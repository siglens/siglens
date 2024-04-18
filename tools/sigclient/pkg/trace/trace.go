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

package trace

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/valyala/fastrand"

	log "github.com/sirupsen/logrus"
)

var envs []string = []string{"prod", "dev"}
var ar_dc []string = []string{"LON", "PAR", "DEN", "PHX", "DFW", "ATL", "BOS", "MHT", "JFK", "CAR", "LHR", "AMS", "FRA", "BOM", "CAL", "DEL", "MAD", "CHE", "FGH", "RTY", "UIO", "MHJ", "HAN", "YHT", "YUL", "MOL", "FOS", "KUN", "SRI", "FGR", "SUN", "PRI", "TAR", "SAR", "ADI", "ERT", "ITR", "DOW", "UQW", "QBF", "POK", "HQZ", "ZAS", "POK", "LIP", "UYQ", "OIK", "TRU", "POL", "NMC", "AZQ"}
var operations []string = []string{"HTTP GET", "HTTP PUT", "HTTP POST", "HTTP PATCH", "HTTP DELETE"}

func randomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes)
}

func StartTraceGeneration(filePrefix string, numTraces int, maxSpans int) {
	log.Println("Starting generation of ", numTraces, " traces...")
	traceCounter := 0
	log.Println("Opening files for services and spans...")
	service_file, err := os.Create(filePrefix + "_services.json")
	service_writer := bufio.NewWriter(service_file)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(filePrefix + "_spans.json")
	w := bufio.NewWriter(f)
	if err != nil {
		log.Fatal(err)
	}
	defer service_file.Close()
	defer f.Close()
	for traceCounter < numTraces {
		var traceId = randomHex(8)
		var randomNumberOfSpans = 1 + fastrand.Uint32n(uint32(maxSpans))
		var prevSpanId = randomHex(8)
		traceRoot := generateRootTrace(traceId)
		serviceAndOperationJson := getServiceAndOperationObject(traceRoot["operationName"], traceRoot["process"])
		writeToFile(traceRoot, w)
		writeToServiceFile(serviceAndOperationJson, service_writer)
		spanCounter := 0
		for spanCounter < int(randomNumberOfSpans) {
			span, spanId := generateChildSpan(traceId, spanCounter, prevSpanId)
			serviceAndOperationJson := getServiceAndOperationObject(span["operationName"], span["process"])
			span["traceID"] = traceId
			writeToFile(span, w)
			writeToServiceFile(serviceAndOperationJson, service_writer)
			spanCounter += 1
			prevSpanId = spanId
		}
		traceCounter += 1
	}
	w.Flush()
	service_writer.Flush()
}

func generateRootTrace(traceId string) map[string]interface{} {
	rootTrace := make(map[string]interface{})
	randNumOperation := fastrand.Uint32n(uint32(len(operations)))
	rootTrace["spanID"] = traceId
	rootTrace["startTime"] = time.Now().UnixNano() / int64(time.Microsecond)
	rootTrace["startTimeMillis"] = time.Now().UnixNano() / int64(time.Millisecond)
	rootTrace["flags"] = 1
	rootTrace["operationName"] = operations[randNumOperation]
	rootTrace["duration"] = fastrand.Uint32n(1_000)
	rootTrace["tags"] = generateTagBody(10)
	rootTrace["process"] = generateProcessTagsBody(10)
	rootTrace["references"] = make([]map[string]interface{}, 0)
	return rootTrace
}

func writeToServiceFile(serviceAndOperationJson map[string]string, w *bufio.Writer) {
	bytes, _ := json.Marshal(serviceAndOperationJson)
	_, err1 := w.Write(bytes)
	if err1 != nil {
		log.Fatal(err1)
	}
	_, err2 := w.WriteString("\n")
	if err2 != nil {
		log.Fatal(err2)
	}
}

func writeToFile(span map[string]interface{}, w *bufio.Writer) {
	bytes, _ := json.Marshal(span)
	_, err1 := w.Write(bytes)
	if err1 != nil {
		log.Fatal(err1)
	}
	_, err2 := w.WriteString("\n")
	if err2 != nil {
		log.Fatal(err2)
	}
}

func getServiceAndOperationObject(operationName, processTags interface{}) map[string]string {
	serviceAndOperationObject := make(map[string]string)
	if opName, ok := operationName.(string); ok {
		serviceAndOperationObject["operationName"] = opName
	}
	if serviceName, ok := processTags.(map[string]interface{})["serviceName"].(string); ok {
		serviceAndOperationObject["serviceName"] = serviceName
	}
	return serviceAndOperationObject
}

func generateChildSpan(traceId string, spanCounter int, prevSpanId string) (map[string]interface{}, string) {
	span := make(map[string]interface{})
	spanId := randomHex(8)
	randNumOperation := fastrand.Uint32n(uint32(len(operations)))
	span["spanID"] = spanId
	span["startTime"] = time.Now().UnixNano() / int64(time.Microsecond)
	span["startTimeMillis"] = time.Now().UnixNano() / int64(time.Millisecond)
	span["flags"] = 1
	span["operationName"] = operations[randNumOperation]
	span["duration"] = fastrand.Uint32n(1_000)
	span["tags"] = generateTagBody(10)
	span["process"] = generateProcessTagsBody(10)
	span["references"] = generateReferenceBody(traceId, spanId, spanCounter, prevSpanId)
	return span, spanId
}

func generateReferenceBody(traceId string, spanId string, spanCounter int, prevSpanId string) []map[string]interface{} {
	references := make([]map[string]interface{}, 1)
	reference := make(map[string]interface{})
	if spanCounter == 0 {
		reference["refType"] = "CHILD_OF"
		reference["spanID"] = traceId
	} else {
		reference["refType"] = "FOLLOWS_FROM"
		reference["spanID"] = prevSpanId
	}
	reference["traceID"] = traceId
	references[0] = reference
	return references
}

func generateProcessTagsBody(n int) map[string]interface{} {
	processBody := make(map[string]interface{})
	randomServiceId := fastrand.Uint32n(10)
	processBody["serviceName"] = fmt.Sprintf("service-%d", randomServiceId)
	listOfTags := make([]map[string]interface{}, 3)
	randomNodeId := fastrand.Uint32n(2_000)
	randomPodId := fastrand.Uint32n(20_000)
	randomUserId := fastrand.Uint32n(2_000_000)
	listOfTags[0] = makeTagObject("node_id", "string", "node-%d", randomNodeId)
	listOfTags[1] = makeTagObject("pod_id", "string", "pod-%d", randomPodId)
	listOfTags[2] = makeTagObject("user_id", "string", "user-%d", randomUserId)
	processBody["tags"] = listOfTags
	return processBody
}

func makeTagObject(s1, s2, s3 string, randomNum uint32) map[string]interface{} {
	tag := make(map[string]interface{})
	tag["key"] = s1
	tag["type"] = s2
	if randomNum == math.MaxInt32 {
		tag["value"] = s3
	} else {
		tag["value"] = fmt.Sprintf(s3, randomNum)
	}
	return tag
}

func generateTagBody(n int) []map[string]interface{} {
	listOfTags := make([]map[string]interface{}, 4)
	randNumrequest := fastrand.Uint32n(2_000_000_0)
	randNumCluster := fastrand.Uint32n(2_000)
	randNumEnv := fastrand.Uint32n(2)
	randNumDc := fastrand.Uint32n(uint32(len(ar_dc)))
	listOfTags[0] = makeTagObject("cluster", "string", "cluster-%d", randNumCluster)
	listOfTags[1] = makeTagObject("env", "string", envs[randNumEnv], math.MaxInt32)
	listOfTags[2] = makeTagObject("dc", "string", ar_dc[randNumDc], math.MaxInt32)
	listOfTags[3] = makeTagObject("request_id", "string", "request-%d", randNumrequest)
	return listOfTags
}
