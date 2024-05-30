package metricsbench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// Payload structure for the request
type Payload struct {
	Start    string    `json:"start"`
	End      string    `json:"end"`
	Queries  []Query   `json:"queries"`
	Formulas []Formula `json:"formulas"`
}

// Query structure
type Query struct {
	Name   string `json:"name"`
	Query  string `json:"query"`
	QlType string `json:"qlType"`
}

// Formula structure
type Formula struct {
	Formula string `json:"formula"`
}

// CreateDataPayload creates the data payload for a single query
func CreateDataPayload(startTime, query, queryName string) ([]byte, error) {
	payload := Payload{
		Start: startTime,
		End:   "now",
		Queries: []Query{
			{
				Name:   queryName,
				Query:  query,
				QlType: "promql",
			},
		},
		Formulas: []Formula{
			{
				Formula: queryName,
			},
		},
	}
	return json.Marshal(payload)
}

// FetchMetrics makes the request for a single query
func FetchMetrics(wg *sync.WaitGroup, destHost, startTime, query, queryName string) {
	defer wg.Done()

	data, err := CreateDataPayload(startTime, query, queryName)
	if err != nil {
		log.Printf("%s error: %v", queryName, err)
		return
	}

	url := fmt.Sprintf("%s/metrics-explorer/api/v1/timeseries", destHost)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		log.Printf("%s error: %v", queryName, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("%s error: %v", queryName, err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("%s error: %v", queryName, err)
		return
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Printf("%s error: %v", queryName, err)
		return
	}

	if series, ok := result["series"].([]interface{}); ok {
		log.Printf("%s: Series Count: %d", queryName, len(series))
	} else {
		log.Printf("%s: Series Count: 0", queryName)
	}
}

func ExecuteMetricsBenchQueries(destHost string) {
	queries := []struct {
		query string
		name  string
	}{
		{query: "avg by (group) (testmetric0{color='gray'})", name: "query1"},
		{query: "max by (car_type) (testmetric1{group='group 0'})", name: "query2"},
		{query: "avg by (car_type) (testmetric2{group='group 0'})", name: "query3"},
		{query: "avg by (group,model) (testmetric3)", name: "query4"},
		{query: "avg by (group,model) (testmetric4)", name: "query5"},
		{query: "max by (group) (testmetric5{group='group 0'})", name: "query6"},
		{query: "sum by (model) (testmetric7{group='group 1'})", name: "query7"},
	}

	timings := []string{
		"now-1h",
		"now-3h",
		"now-6h",
		"now-12h",
		"now-24h",
		"now-2d",
		"now-7d",
		"now-30d",
		"now-90d",
	}

	for _, timeRange := range timings {
		fmt.Printf("Time: %s\n", timeRange)
		reqStartTime := time.Now()

		var wg sync.WaitGroup
		for _, q := range queries {
			wg.Add(1)
			go FetchMetrics(&wg, destHost, timeRange, q.query, q.name)
		}
		wg.Wait()

		reqEndTime := time.Now()
		fmt.Printf("Time taken for last (%s): %.2fs\n", timeRange, reqEndTime.Sub(reqStartTime).Seconds())
	}
}
