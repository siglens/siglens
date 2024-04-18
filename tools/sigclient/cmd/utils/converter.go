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

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func convertTSVToJson(input string, output string) error {
	sTime := time.Now()
	file, err := os.Open(input)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return err
	}
	columns := strings.Split(scanner.Text(), "\t")
	log.Infof("Assuming columns %+v for file %+v", columns, input)

	jsonValue := make(map[string]interface{}, len(columns))
	for _, column := range columns {
		jsonValue[column] = nil
	}

	outFile, err := os.OpenFile(output, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer outFile.Close()

	count := 0
	for scanner.Scan() {
		count++

		if count%1000 == 0 {
			log.Infof("Still converting file. Total time %+v. Number of lines %+v", time.Since(sTime), count)
		}
		rawValues := strings.Split(scanner.Text(), "\t")
		for idx, column := range columns {
			if idx >= len(rawValues) {
				jsonValue[column] = nil
			} else {
				value := rawValues[idx]

				// Try converting to a number.
				intVal, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					jsonValue[column] = intVal
					continue
				}

				floatVal, err := strconv.ParseFloat(value, 64)
				if err == nil {
					jsonValue[column] = floatVal
					continue
				}

				// Try converting to a boolean.
				if value == "false" {
					jsonValue[column] = false
					continue
				}
				if value == "true" {
					jsonValue[column] = true
					continue
				}

				// Keep this as a string.
				jsonValue[column] = value
			}
		}
		rawVal, err := json.Marshal(jsonValue)
		if err != nil {
			log.Errorf("Failed to marshal json! %v", err)
			continue
		}
		_, _ = outFile.Write(rawVal)
		_, _ = outFile.WriteString("\n")
	}
	log.Infof("Finished converting file. Total time %+v. Number of lines %+v", time.Since(sTime), count)
	return scanner.Err()
}

func main() {
	inputPtr := flag.String("input", "", "input tsv file to convert")
	outputPtr := flag.String("output", "", "output of json file")

	flag.Parse()
	inputStr, outputStr := *inputPtr, *outputPtr

	if len(inputStr) == 0 {
		log.Fatal("Input file cannot be empty")
	}

	if outputStr == inputStr {
		log.Fatalf("Input string %+v cannot be the same as the output string %+v", inputStr, outputStr)
	}
	if len(outputStr) == 0 {
		outputStr = inputStr + ".json"
	}
	log.Infof("Converting %+v to a json file at %+v:", inputStr, outputStr)
	err := convertTSVToJson(inputStr, outputStr)
	if err != nil {
		log.Fatalf("Failed to convert %+v to json: %v", inputStr, err)
	}
}
