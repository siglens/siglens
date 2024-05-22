package query

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

func RunPromQLQueryFromFile(apiURL string, filepath string) {
	log.Infof("RunPromQLQueryFromFile: Reading queries from file: %v", filepath)
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("RunPromQLQueryFromFile: Error opening file: %v, err: %v", filepath, err)
		return
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	// Skip the first line comment
	_, err = csvReader.Read()
	if err != nil {
		log.Fatalf("RunPromQLQueryFromFile: Error reading file header: %v, err: %v", filepath, err)
		return
	}

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("RunPromQLQueryFromFile: Error reading file: %v, err: %v", filepath, err)
			return
		}

		if len(rec) != 5 {
			log.Fatalf("RunPromQLQueryFromFile: Invalid number of columns in query file: [%v]. Expected 5", rec)
			return
		}

		query, step := rec[0], rec[3]

		nowTs := uint64(time.Now().Unix())

		start := utils.ConvertStringToEpochSec(nowTs, rec[1], nowTs-3600)
		end := utils.ConvertStringToEpochSec(nowTs, rec[2], nowTs)
		expectedResult, err := strconv.ParseBool(rec[4])
		if err != nil {
			log.Fatalf("RunPromQLQueryFromFile: Invalid value for expected result: %v", rec[4])
			return
		}

		success := RunPromQLQuery(apiURL, query, fmt.Sprintf("%v", start), fmt.Sprintf("%v", end), step, expectedResult)
		if success != expectedResult {
			log.Fatalf("Query: %v, expected result: %v, actual result: %v", query, expectedResult, success)
		}
	}
	log.Info("RunPromQLQueryFromFile: All queries executed successfully")
}

func RunPromQLQuery(apiURL string, query string, start string, end string, step string, expectedResult bool) (success bool) {
	u, err := url.Parse(apiURL)
	if err != nil {
		log.Fatalf("Error parsing URL: %v, err: %v", apiURL, err)
		return false
	}

	params := url.Values{}
	params.Add("query", query)
	params.Add("start", start)
	params.Add("end", end)
	params.Add("step", step)
	u.RawQuery = params.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		log.Fatalf("Error sending GET request: %v, err: %v", u.String(), err)
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v, err: %v", u.String(), err)
		return false
	}

	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if result["status"] == "success" {
		log.Printf("Query: %v was successful.", query)
		return true
	} else if expectedResult {
		log.Errorf("Query: %v, Expected: %v, Actual: %v, status code: %v, response body: %v", query, expectedResult, success, resp.StatusCode, string(body))
	} else {
		log.Printf("Query: %v, Expected: Fail, Actual: Fail", query)
	}
	return false
}
