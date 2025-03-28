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

package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"verifier/pkg/alerts"
	"verifier/pkg/ingest"
	"verifier/pkg/metricsbench"
	"verifier/pkg/query"
	"verifier/pkg/trace"
	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest",
	Run: func(cmd *cobra.Command, args []string) {
		log.Fatal("Ingestion command should be used with esbulk / metrics.")
	},
}

// esBulkCmd represents the es bulk ingestion
var esBulkCmd = &cobra.Command{
	Use:   "esbulk",
	Short: "ingest records to SigScalr using es bulk",
	Run: func(cmd *cobra.Command, args []string) {
		processCount, _ := cmd.Flags().GetInt("processCount")
		dest, _ := cmd.Flags().GetString("dest")
		totalEvents, _ := cmd.Flags().GetInt("totalEvents")
		continuous, _ := cmd.Flags().GetBool("continuous")
		batchSize, _ := cmd.Flags().GetInt("batchSize")
		indexPrefix, _ := cmd.Flags().GetString("indexPrefix")
		numIndices, _ := cmd.Flags().GetInt("numIndices")
		generatorType, _ := cmd.Flags().GetString("generator")
		ts, _ := cmd.Flags().GetBool("timestamp")
		dataFile, _ := cmd.Flags().GetString("filePath")
		indexName, _ := cmd.Flags().GetString("indexName")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")
		eventsPerDay, _ := cmd.Flags().GetUint64("eventsPerDay")
		maxColumns, _ := cmd.Flags().GetUint32("maxColumns")
		minColumns, _ := cmd.Flags().GetUint32("minColumns")
		uniqColumns, _ := cmd.Flags().GetUint32("uniqColumns")
		enableVariableNumColumns, _ := cmd.Flags().GetBool("enableVariableNumColumns")

		if eventsPerDay > 0 {
			if cmd.Flags().Changed("totalEvents") {
				log.Fatalf("You cannot use totalEvents and eventsPerDay together; you must choose one.")
				return
			}
			continuous = true
		}

		var dataGeneratorConfig *utils.GeneratorDataConfig
		if enableVariableNumColumns {
			if maxColumns == 0 {
				log.Fatalf("maxColumns must be greater than 0")
				return
			}
			if uniqColumns != 0 && uniqColumns < maxColumns {
				log.Fatalf("uniqColumns must be greater than or equal to maxColumns")
				return
			}
			dataGeneratorConfig = ingest.GetGeneratorDataConfig(int(maxColumns), enableVariableNumColumns, int(minColumns), int(uniqColumns))
		}

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v. Continuous: %+v\n", totalEvents, continuous)
		log.Infof("batchSize : %+v\n", batchSize)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("indexName : %+v\n", indexName)
		log.Infof("numIndices : %+v\n", numIndices)
		log.Infof("bearerToken : %+v\n", bearerToken)
		log.Infof("generatorType : %+v. Add timestamp: %+v\n", generatorType, ts)
		log.Infof("eventsPerDay : %+v\n", eventsPerDay)
		log.Infof("enableVariableNumColumns : %+v\n", enableVariableNumColumns)
		if enableVariableNumColumns {
			log.Infof("maxColumns : %+v\n", dataGeneratorConfig.MaxColumns)
			log.Infof("minColumns : %+v\n", dataGeneratorConfig.MinColumns)
		}

		ingest.StartIngestion(ingest.ESBulk, generatorType, dataFile, totalEvents, continuous, batchSize, dest, indexPrefix, indexName, numIndices, processCount, ts, 0, bearerToken, 0, eventsPerDay, dataGeneratorConfig, nil)
	},
}

var functionalTestCmd = &cobra.Command{
	Use:   "functional",
	Short: "functional testing of SigLens",
	Run: func(cmd *cobra.Command, args []string) {
		dest, _ := cmd.Flags().GetString("dest")
		queryDest, _ := cmd.Flags().GetString("queryDest")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")
		filePath, _ := cmd.Flags().GetString("queriesToRunFile")
		longer, _ := cmd.Flags().GetBool("longer")
		doIngest, _ := cmd.Flags().GetBool("doIngest")
		doQuery, _ := cmd.Flags().GetBool("doQuery")

		log.Infof("dest : %+v\n", dest)
		log.Infof("queryDest : %+v\n", queryDest)
		log.Infof("bearerToken : %+v\n", bearerToken)
		log.Infof("queriesToRunFile : %+v\n", filePath)
		log.Infof("longer : %+v\n", longer)
		log.Infof("doIngest : %+v\n", doIngest)
		log.Infof("doQuery : %+v\n", doQuery)

		totalEvents := 100_000
		batchSize := 100
		numIndices := 10
		processCount := 1
		indexPrefix := "ind"
		indexName := ""
		numFixedCols := 100
		maxVariableCols := 20
		sleepDuration := 15 * time.Second

		if longer {
			totalEvents = 20_000_000
			batchSize = 100
			numIndices = 10
			processCount = 10
			sleepDuration = 30 * time.Second
		}

		if doIngest {

			dataGeneratorConfig := utils.InitFunctionalTestGeneratorDataConfig(numFixedCols, maxVariableCols)

			ingest.StartIngestion(ingest.ESBulk, "functional", "", totalEvents, false, batchSize, dest, indexPrefix,
				indexName, numIndices, processCount, true, 0, bearerToken, 0, 0, dataGeneratorConfig, nil)

			err := query.MigrateLookups([]string{"../../cicd/test_lookup.csv"})
			if err != nil {
				log.Fatalf("Error while migrating lookups: %v", err)
				return
			}

			if doQuery {
				time.Sleep(sleepDuration)
			}
		}

		if doQuery {
			query.FunctionalTest(queryDest, filePath)
		}
	},
}

var performanceTestCmd = &cobra.Command{
	Use:   "performance",
	Short: "performance testing of SigLens",
	Run: func(cmd *cobra.Command, args []string) {
		dest, _ := cmd.Flags().GetString("dest")
		queryDest, _ := cmd.Flags().GetString("queryDest")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")

		log.Infof("dest : %+v\n", dest)
		log.Infof("queryDest : %+v\n", queryDest)
		log.Infof("bearerToken : %+v\n", bearerToken)

		totalEvents := 1_000_000_000
		batchSize := 50
		numIndices := 10
		processCount := 10
		indexPrefix := "ind"
		indexName := ""
		numFixedCols := 100
		maxVariableCols := 20
		concurrentQueries := 5

		logChan := make(chan utils.Log, 1000)

		dataGenConfig := utils.InitPerformanceTestGeneratorDataConfig(numFixedCols, maxVariableCols, logChan)

		ctx, cancel := context.WithCancel(context.Background())

		go func(cancel context.CancelFunc) {
			defer cancel()
			// addTs should be false for performance testing
			ingest.StartIngestion(ingest.ESBulk, "performance", "", totalEvents, false, batchSize, dest, indexPrefix,
				indexName, numIndices, processCount, false, 0, bearerToken, 0, 0, dataGenConfig, nil)
		}(cancel)

		go query.PerformanceTest(ctx, logChan, queryDest, concurrentQueries, utils.GetVariablesColsNamesFromConfig(dataGenConfig))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	},
}

var concurrentQueriesTestCmd = &cobra.Command{
	Use:   "concurrentQueries",
	Short: "testing concurrent queries on SigLens",
	Run: func(cmd *cobra.Command, args []string) {
		dest, _ := cmd.Flags().GetString("dest")
		queryText, _ := cmd.Flags().GetString("queryText")
		numOfConcurrentQueries, _ := cmd.Flags().GetInt("numOfConcurrentQueries")
		iterations, _ := cmd.Flags().GetInt("iterations")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")

		log.Infof("dest : %+v\n", dest)
		log.Infof("queryDest : %+v\n", dest)
		log.Infof("queryText : %+v\n", queryText)
		log.Infof("numOfConcurrentQueries : %+v\n", numOfConcurrentQueries)
		log.Infof("iterations : %+v\n", iterations)
		log.Infof("bearerToken : %+v\n", bearerToken)

		query.RunConcurrentQueries(dest, queryText, numOfConcurrentQueries, iterations)
	},
}

var clickBenchTestCmd = &cobra.Command{
	Use:   "clickBench",
	Short: "testing clickBench queries on SigLens",
	Run: func(cmd *cobra.Command, args []string) {
		dest, _ := cmd.Flags().GetString("dest")
		thresholdFactor, _ := cmd.Flags().GetFloat64("thresholdFactor")

		log.Infof("dest : %+v\n", dest)

		queriesAndRespTimes, err := query.GetClickBenchQueriesAndRespTimes()
		if err != nil {
			log.Fatalf("Error getting clickbench queries: %v", err)
			return
		}

		query.ValidateClickBenchQueries(dest, queriesAndRespTimes, thresholdFactor)
	},
}

// metricsIngestCmd represents the metrics ingestion
var metricsIngestCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Ingest metrics into Siglens",
	Run: func(cmd *cobra.Command, args []string) {
		processCount, _ := cmd.Flags().GetInt("processCount")
		dest, _ := cmd.Flags().GetString("dest")
		totalEvents, _ := cmd.Flags().GetInt("totalEvents")
		continuous, _ := cmd.Flags().GetBool("continuous")
		batchSize, _ := cmd.Flags().GetInt("batchSize")
		nMetrics, _ := cmd.Flags().GetInt("metrics")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")
		generatorType, _ := cmd.Flags().GetString("generator")
		cardinality, _ := cmd.Flags().GetUint64("cardinality")
		eventsPerDay, _ := cmd.Flags().GetUint64("eventsPerDay")
		metricsFormat, _ := cmd.Flags().GetString("metricsFormat")

		if eventsPerDay > 0 {
			if cmd.Flags().Changed("totalEvents") {
				log.Fatalf("You cannot use totalEvents and eventsPerDay together; you must choose one.")
				return
			}
			continuous = true
		}

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v. Continuous: %+v\n", totalEvents, continuous)
		log.Infof("batchSize : %+v. Num metrics: %+v\n", batchSize, nMetrics)
		log.Infof("bearerToken : %+v\n", bearerToken)
		log.Infof("generatorType : %+v.\n", generatorType)
		log.Infof("cardinality : %+v.\n", cardinality)
		log.Infof("eventsPerDay : %+v\n", eventsPerDay)
		log.Infof("metricsFormat : %+v\n", metricsFormat)

		var ingestFormat ingest.IngestType
		switch strings.ToLower(metricsFormat) {
		case strings.ToLower(ingest.OpenTSDB.String()):
			ingestFormat = ingest.OpenTSDB
		case strings.ToLower(ingest.PrometheusRemoteWrite.String()):
			ingestFormat = ingest.PrometheusRemoteWrite
		default:
			log.Fatalf("Invalid metric format: %s. Supported metric formats: otsdb, prometheus", metricsFormat)
			return
		}
		ingest.StartIngestion(ingestFormat, generatorType, "", totalEvents, continuous, batchSize, dest, "", "", 0, processCount, false, nMetrics, bearerToken, cardinality, eventsPerDay, nil, nil)
	},
}

var esQueryCmd = &cobra.Command{
	Use:   "esbulk",
	Short: "send esbulk queries to SigScalr",
	Run: func(cmd *cobra.Command, args []string) {
		dest, _ := cmd.Flags().GetString("dest")
		numIterations, _ := cmd.Flags().GetInt("numIterations")
		verbose, _ := cmd.Flags().GetBool("verbose")
		continuous, _ := cmd.Flags().GetBool("continuous")
		indexPrefix, _ := cmd.Flags().GetString("indexPrefix")
		filepath, _ := cmd.Flags().GetString("filePath")
		randomQueries, _ := cmd.Flags().GetBool("randomQueries")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")
		outputFile, _ := cmd.Flags().GetString("outputFile")
		runResponseTime, _ := cmd.Flags().GetBool("runResponseTime")
		lookupFiles, _ := cmd.Flags().GetStringSlice("lookups")

		log.Infof("dest : %+v\n", dest)
		log.Infof("numIterations : %+v\n", numIterations)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("verbose : %+v\n", verbose)
		log.Infof("continuous : %+v\n", continuous)
		log.Infof("filePath : %+v\n", filepath)
		log.Infof("randomQueries: %+v\n", randomQueries)
		log.Infof("bearerToken : %+v\n", bearerToken)

		err := query.MigrateLookups(lookupFiles)
		if err != nil {
			log.Fatalf("Error while migrating lookups: %v", err)
			return
		}

		if filepath != "" {
			if runResponseTime {
				query.RunQueryFromFileAndOutputResponseTimes(dest, filepath, outputFile)
			} else {
				query.RunQueryFromFile(dest, numIterations, indexPrefix, continuous, verbose, filepath, bearerToken)
			}
		} else {
			query.StartQuery(dest, numIterations, indexPrefix, continuous, verbose, randomQueries, bearerToken)
		}
	},
}

var metricsBenchCmd = &cobra.Command{
	Use:   "metricsbench",
	Short: "send metricsbench queries to SigScalr",
	Run: func(cmd *cobra.Command, args []string) {
		destHost, _ := cmd.Flags().GetString("dest")
		numQueryIterations, _ := cmd.Flags().GetInt("numQueryIterations")
		log.Infof("destHost : %+v\n", destHost)
		log.Infof("numQueryIterations : %+v.\n", numQueryIterations)
		if destHost == "" {
			log.Fatalf("destUrl is required")
		}
		metricsbench.ExecuteMetricsBenchQueries(destHost, numQueryIterations)
	},
}

var alertsCmd = &cobra.Command{
	Use:   "alerts",
	Short: "alerts",
	Run: func(cmd *cobra.Command, args []string) {
		log.Fatal("Alerts command should be used with e2e/load-test.")
	},
}

var alertsE2ECmd = &cobra.Command{
	Use:   "e2e",
	Short: "Perform E2E tests for alerts: From creating contact points, creating alerts, sending alerts, and verifying alerts, and finally cleanup",
	Run: func(cmd *cobra.Command, args []string) {
		destHost, _ := cmd.Flags().GetString("dest")
		log.Infof("destHost : %+v\n", destHost)
		if destHost == "" {
			log.Fatalf("Destination Host is required")
		}
		alerts.RunAlertsTest(destHost)
	},
}

var alertsLoadTestCmd = &cobra.Command{
	Use:   "load-test",
	Short: "Perform Load test for alerts: Create `n` number of alerts, send alerts, and verify alerts",
	Run: func(cmd *cobra.Command, args []string) {
		destHost, _ := cmd.Flags().GetString("dest")
		numAlerts, _ := cmd.Flags().GetUint64("numAlerts")
		runVector, _ := cmd.Flags().GetInt8("runVector")
		cleanup, _ := cmd.Flags().GetBool("cleanup")
		log.Infof("destHost : %+v\n", destHost)

		if cleanup {
			alerts.RunAlertsCleanup(destHost)
			return
		}

		log.Infof("numAlerts : %+v\n", numAlerts)
		if destHost == "" {
			log.Fatalf("Destination Host is required")
		}
		if numAlerts == 0 {
			log.Fatalf("Number of alerts must be greater than 0")
		}
		if runVector > 1 {
			runVector = 1
		} else if runVector < -1 {
			runVector = -1
		}

		alerts.RunAlertsLoadTest(destHost, numAlerts, runVector)
	},
}

var cmdWrap wrapper

type wrapper struct {
	err error
}

// RunE fails to proceed further in case of error resulting in not executing PostRun actions
func (w *wrapper) Run(f func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		err := f(cmd, args)
		w.err = err
	}
}

var metricsQueryCmd = &cobra.Command{
	Use:   "otsdb",
	Short: "send otsdb queries to SigScalr",
	Run: cmdWrap.Run(func(cmd *cobra.Command, args []string) error {
		dest, _ := cmd.Flags().GetString("dest")
		numIterations, _ := cmd.Flags().GetInt("numIterations")
		verbose, _ := cmd.Flags().GetBool("verbose")
		continuous, _ := cmd.Flags().GetBool("continuous")
		validateMetricsOutput, _ := cmd.Flags().GetBool("validateMetricsOutput")
		filepath, _ := cmd.Flags().GetString("filePath")

		log.Infof("dest : %+v\n", dest)
		log.Infof("numIterations : %+v\n", numIterations)
		log.Infof("verbose : %+v\n", verbose)
		log.Infof("continuous : %+v\n", continuous)
		log.Infof("validateMetricsOutput : %+v\n", validateMetricsOutput)
		log.Infof("filePath : %+v\n", filepath)

		if filepath != "" {
			query.RunMetricQueryFromFile(dest, filepath)
		} else {
			resTS := query.StartMetricsQuery(dest, numIterations, continuous, verbose, validateMetricsOutput)
			for k, v := range resTS {
				if !v {
					log.Errorf("metrics query has no results for query type: %s", k)
					return fmt.Errorf("metrics query has no results for query type: %s", k)
				}
			}
		}
		return nil
	}),
}

var promQLQueryCmd = &cobra.Command{
	Use:   "promql",
	Short: "send promql queries to SigScalr",
	Run: cmdWrap.Run(func(cmd *cobra.Command, args []string) error {
		dest, _ := cmd.Flags().GetString("dest")
		filepath, _ := cmd.Flags().GetString("filePath")
		promQLQueryWithStartEnd, _ := cmd.Flags().GetString("query")
		var promQLQuery, startEpoch, endEpoch string
		if promQLQueryWithStartEnd != "" {
			parts := strings.SplitN(promQLQueryWithStartEnd, "start:", 2)
			if len(parts) != 2 {
				log.Fatalf("Invalid query format: %v. Expected format: <query> start:<startEpoch> end:<endEpoch>", promQLQueryWithStartEnd)
			}

			promQLQuery = strings.TrimSpace(parts[0])
			parts = strings.SplitN(parts[1], "end:", 2)

			if len(parts) != 2 {
				log.Fatalf("Invalid query format: %v. Expected format: <query> start:<startEpoch> end:<endEpoch>", promQLQueryWithStartEnd)
			}

			startEpoch = strings.TrimSpace(parts[0])
			endEpoch = strings.TrimSpace(parts[1])
		}
		log.Infof("dest : %+v\n", dest)
		log.Infof("filePath : %+v\n", filepath)
		log.Infof("query : %+v\n", promQLQuery)

		if filepath != "" {
			query.RunPromQLQueryFromFile(dest, filepath)
		} else if promQLQuery != "" {
			res := query.RunPromQLQuery(dest, promQLQuery, startEpoch, endEpoch, "15", true)
			if !res {
				log.Errorf("PromQL query failed")
				return fmt.Errorf("PromQL query failed")
			}
		}
		return nil
	}),
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "send queries to SigScalr",
	Run: func(cmd *cobra.Command, args []string) {
		log.Fatal("Query command should be used with esbulk / metrics.")
	},
}

var traceCmd = &cobra.Command{
	Use:   "traces",
	Short: "send traces to Sigscalr",
	Run: func(cmd *cobra.Command, args []string) {
		filePrefix, _ := cmd.Flags().GetString("filePrefix")
		totalTraces, _ := cmd.Flags().GetInt("totalEvents")
		maxSpans, _ := cmd.Flags().GetInt("maxSpans")

		log.Infof("file : %+v\n", filePrefix)
		log.Infof("totalTraces : %+v\n", totalTraces)
		log.Infof("maxSpans : %+v\n", maxSpans)
		trace.StartTraceGeneration(filePrefix, totalTraces, maxSpans)
	},
}

func unwrap[T any](value T, err error) T {
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	return value
}

var longevityCmd = &cobra.Command{
	Use:   "longevity",
	Short: "Run longevity tests",
	Long:  "Continuously ingest and query data until stopped",
	Run: func(cmd *cobra.Command, args []string) {
		generatorType := "benchmark"
		dataFile := ""
		totalEvents := 0
		continuous := true
		batchSize := 100
		ingestUrl, _ := cmd.Flags().GetString("dest")
		queryUrl, _ := cmd.Flags().GetString("queryDest")
		indexPrefix := "longevity-test"
		indexName := ""
		numIndices := 1
		processCount := 1
		addTs := true
		numMetrics := 0
		bearerToken, _ := cmd.Flags().GetString("bearerToken")
		eventsPerDay := uint64(100_000_000)

		minCols := 50
		maxCols := 150
		uniqueCols := 0
		dataGeneratorConfig := utils.InitGeneratorDataConfig(maxCols, true, minCols, uniqueCols)

		failOnError, _ := cmd.Flags().GetBool("failOnError")

		templates := []*query.QueryTemplate{
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(unwrap(query.Filter("city_c1", "Boston")), "", 10, 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(unwrap(query.Filter("city_c1", "Boston")), "", 10, 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(unwrap(query.Filter("city_c1", "New *")), "", 10, 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(unwrap(query.Filter("city_c1", "New *")), "", 10, 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(unwrap(query.Filter("state_c1", "Texas")), "latency_c1", 10, 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(unwrap(query.Filter("state_c1", "Texas")), "latency_c1", 10, 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(unwrap(query.Filter("app_version_c1", "1.2.3")), 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(unwrap(query.Filter("app_version_c1", "1.2.3")), 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(unwrap(query.Filter("app_version_c1", "1.2.3")), 0, 0)), 3600, 10),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(unwrap(query.Filter("app_version_c1", "1.2.3")), 0, 0)), 6*3600, 100),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(unwrap(query.Filter("state_c1", "Texas")), 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(unwrap(query.Filter("state_c1", "Texas")), 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(query.MatchAll(), 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(query.MatchAll(), 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(query.DynamicFilter(), 0, 0)), 300, 10),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(query.DynamicFilter(), 0, 0)), 3600, 10),
			query.NewQueryTemplate(unwrap(query.NewCountQueryValidator(query.DynamicFilter(), 0, 0)), 6*3600, 100),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(query.DynamicFilter(), "", 10, 0, 0)), 1, 1),
			query.NewQueryTemplate(unwrap(query.NewFilterQueryValidator(query.DynamicFilter(), "", 100, 0, 0)), 300, 10),
		}
		maxConcurrentQueries := int32(1)
		queryManager := query.NewQueryManager(templates, maxConcurrentQueries, queryUrl, failOnError)

		callback := func(logs []map[string]interface{}) {
			queryManager.HandleIngestedLogs(logs)
		}

		ingest.StartIngestion(ingest.ESBulk, generatorType, dataFile,
			totalEvents, continuous, batchSize, ingestUrl, indexPrefix, indexName,
			numIndices, processCount, addTs, numMetrics, bearerToken, 0,
			eventsPerDay, dataGeneratorConfig, callback)
	},
}

func init() {
	rootCmd.PersistentFlags().StringP("dest", "d", "", "Server URL.")
	rootCmd.PersistentFlags().StringP("bearerToken", "r", "", "Bearer token")

	concurrentQueriesTestCmd.PersistentFlags().IntP("numOfConcurrentQueries", "c", 1, "Number of concurrent queries to run")
	concurrentQueriesTestCmd.PersistentFlags().IntP("iterations", "i", 1, "Number of iterations to run")
	concurrentQueriesTestCmd.PersistentFlags().StringP("queryText", "q", "", "Query to run")

	clickBenchTestCmd.PersistentFlags().Float64P("thresholdFactor", "t", 1.5, "Threshold factor for clickbench queries")

	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from.")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events to send")
	ingestCmd.PersistentFlags().BoolP("continuous", "c", false, "Continous ingestion will ingore -t and will constantly send events as fast as possible")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 100, "Batch size")
	ingestCmd.PersistentFlags().StringP("indexPrefix", "i", "ind", "index prefix")
	ingestCmd.PersistentFlags().Uint64P("eventsPerDay", "e", 0, "Number of events per day")
	ingestCmd.PersistentFlags().StringP("indexName", "a", "", "index name")

	esBulkCmd.Flags().BoolP("timestamp", "s", false, "Add timestamp in payload")
	esBulkCmd.PersistentFlags().IntP("numIndices", "n", 1, "number of indices to ingest to")
	esBulkCmd.PersistentFlags().StringP("generator", "g", "dynamic-user", "type of generator to use. Options=[static,dynamic-user,file]. If file is selected, -x/--filePath must be specified")
	esBulkCmd.PersistentFlags().StringP("filePath", "x", "", "path to json file to use as logs")
	esBulkCmd.PersistentFlags().Uint32P("maxColumns", "", 100, "maximum number of columns to generate. Default is 100")
	esBulkCmd.PersistentFlags().Uint32P("minColumns", "", 0, "minimum number of columns to generate. Default is 0. if 0, it will be set to maxColumns")
	esBulkCmd.PersistentFlags().Uint32P("uniqColumns", "", 0, "unique column names to generate")
	esBulkCmd.PersistentFlags().BoolP("enableVariableNumColumns", "", false, "generate a variable number of columns per record. Each record will have a random number of columns between minColumns and maxColumns")

	functionalTestCmd.PersistentFlags().StringP("queryDest", "q", "", "Query Server Address, format is IP:PORT")
	functionalTestCmd.PersistentFlags().StringP("queriesToRunFile", "f", "", "Path of the file containing paths of functional query files to be tested")
	functionalTestCmd.PersistentFlags().BoolP("longer", "l", false, "Run longer functional test")
	functionalTestCmd.PersistentFlags().BoolP("doIngest", "i", true, "Perfom ingestion for functional Test")
	functionalTestCmd.PersistentFlags().BoolP("doQuery", "u", true, "Perfom query for functional Test")

	performanceTestCmd.PersistentFlags().StringP("queryDest", "q", "", "Query Server Address, format is IP:PORT")

	metricsIngestCmd.PersistentFlags().IntP("metrics", "m", 1_000, "Number of different metric names to send")
	metricsIngestCmd.PersistentFlags().StringP("generator", "g", "dynamic-user", "type of generator to use. Options=[static,dynamic-user,file]. If file is selected, -x/--filePath must be specified")
	metricsIngestCmd.PersistentFlags().Uint64P("cardinality", "u", 2_000_000, "Specify the total unique time series (cardinality).")
	metricsIngestCmd.PersistentFlags().StringP("metricsFormat", "f", "otsdb", "Specify metrics format. Options=[otsdb, prometheus]")

	queryCmd.PersistentFlags().IntP("numIterations", "n", 10, "number of times to run entire query suite")
	queryCmd.PersistentFlags().BoolP("verbose", "v", false, "Verbose querying will output raw docs returned by queries")
	queryCmd.PersistentFlags().BoolP("continuous", "c", false, "Continuous querying will ignore -c and -v and will continuously send queries to the destination")
	queryCmd.PersistentFlags().BoolP("validateMetricsOutput", "y", false, "check if metric querries return any results")
	queryCmd.PersistentFlags().StringP("filePath", "f", "", "filepath to csv file to use to run queries from")
	queryCmd.PersistentFlags().BoolP("randomQueries", "", false, "generate random queries")
	queryCmd.PersistentFlags().StringP("query", "q", "", "promql query to run")
	queryCmd.PersistentFlags().StringP("outputFile", "o", "", "The filePath to output the Response time of Queries in csv format, When runResponseTime is set to True.")
	queryCmd.PersistentFlags().BoolP("runResponseTime", "t", false, "Runs the given Queries and outputs the response time into a file in CSV format.")
	queryCmd.PersistentFlags().StringSliceP("lookups", "l", []string{}, "List of lookups to migrate")
	queryCmd.PersistentFlags().StringP("indexPrefix", "i", "ind", "index prefix")

	traceCmd.PersistentFlags().StringP("filePrefix", "f", "", "Name of file to output to")
	traceCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of traces to generate")
	traceCmd.Flags().IntP("maxSpans", "s", 100, "max number of spans in a single trace")

	metricsBenchCmd.PersistentFlags().IntP("numQueryIterations", "n", 1, "Number of query iterations loop")

	alertsLoadTestCmd.PersistentFlags().Uint64P("numAlerts", "n", 1, "Number of alerts to create")
	alertsLoadTestCmd.PersistentFlags().Int8P("runVector", "v", 0, "Run vector: -1 - Explicitly Disable running of Vector, 0 - Optional State: Will try to run vector, but will not stop the test if encountered and issue with Vector, 1 - Explictly Enable running of Vector")
	alertsLoadTestCmd.PersistentFlags().BoolP("cleanup", "c", false, "Cleanup alerts. If this is set true, it will only cleanup alerts and not run the load test")

	longevityCmd.PersistentFlags().StringP("queryDest", "q", "", "Query Server Address")
	longevityCmd.PersistentFlags().BoolP("failOnError", "f", false, "Fail on first error")

	queryCmd.AddCommand(esQueryCmd)
	queryCmd.AddCommand(metricsQueryCmd)
	queryCmd.AddCommand(promQLQueryCmd)
	ingestCmd.AddCommand(esBulkCmd)
	ingestCmd.AddCommand(metricsIngestCmd)
	alertsCmd.AddCommand(alertsLoadTestCmd)
	alertsCmd.AddCommand(alertsE2ECmd)

	rootCmd.AddCommand(ingestCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(functionalTestCmd)
	rootCmd.AddCommand(performanceTestCmd)
	rootCmd.AddCommand(traceCmd)
	rootCmd.AddCommand(metricsBenchCmd)
	rootCmd.AddCommand(alertsCmd)
	rootCmd.AddCommand(concurrentQueriesTestCmd)
	rootCmd.AddCommand(clickBenchTestCmd)
	rootCmd.AddCommand(longevityCmd)
}
