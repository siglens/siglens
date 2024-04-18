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
	"fmt"
	"verifier/pkg/ingest"
	"verifier/pkg/query"
	"verifier/pkg/trace"

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

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v. Continuous: %+v\n", totalEvents, continuous)
		log.Infof("batchSize : %+v\n", batchSize)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("indexName : %+v\n", indexName)
		log.Infof("numIndices : %+v\n", numIndices)
		log.Infof("bearerToken : %+v\n", bearerToken)
		log.Infof("generatorType : %+v. Add timestamp: %+v\n", generatorType, ts)

		ingest.StartIngestion(ingest.ESBulk, generatorType, dataFile, totalEvents, continuous, batchSize, dest, indexPrefix, indexName, numIndices, processCount, ts, 0, bearerToken)
	},
}

// metricsIngestCmd represents the metrics ingestion
var metricsIngestCmd = &cobra.Command{
	Use:   "metrics",
	Short: "ingest metrics to /api/put in OTSDB format",
	Run: func(cmd *cobra.Command, args []string) {
		processCount, _ := cmd.Flags().GetInt("processCount")
		dest, _ := cmd.Flags().GetString("dest")
		totalEvents, _ := cmd.Flags().GetInt("totalEvents")
		continuous, _ := cmd.Flags().GetBool("continuous")
		batchSize, _ := cmd.Flags().GetInt("batchSize")
		nMetrics, _ := cmd.Flags().GetInt("metrics")
		bearerToken, _ := cmd.Flags().GetString("bearerToken")

		log.Infof("processCount : %+v\n", processCount)
		log.Infof("dest : %+v\n", dest)
		log.Infof("totalEvents : %+v. Continuous: %+v\n", totalEvents, continuous)
		log.Infof("batchSize : %+v. Num metrics: %+v\n", batchSize, nMetrics)
		log.Infof("bearerToken : %+v\n", bearerToken)
		ingest.StartIngestion(ingest.OpenTSDB, "", "", totalEvents, continuous, batchSize, dest, "", "", 0, processCount, false, nMetrics, bearerToken)
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

		log.Infof("dest : %+v\n", dest)
		log.Infof("numIterations : %+v\n", numIterations)
		log.Infof("indexPrefix : %+v\n", indexPrefix)
		log.Infof("verbose : %+v\n", verbose)
		log.Infof("continuous : %+v\n", continuous)
		log.Infof("filePath : %+v\n", filepath)
		log.Infof("randomQueries: %+v\n", randomQueries)
		log.Infof("bearerToken : %+v\n", bearerToken)
		if filepath != "" {
			query.RunQueryFromFile(dest, numIterations, indexPrefix, continuous, verbose, filepath, bearerToken)
		} else {
			query.StartQuery(dest, numIterations, indexPrefix, continuous, verbose, randomQueries, bearerToken)
		}
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

		log.Infof("dest : %+v\n", dest)
		log.Infof("numIterations : %+v\n", numIterations)
		log.Infof("verbose : %+v\n", verbose)
		log.Infof("continuous : %+v\n", continuous)
		log.Infof("validateMetricsOutput : %+v\n", validateMetricsOutput)
		resTS := query.StartMetricsQuery(dest, numIterations, continuous, verbose, validateMetricsOutput)
		for k, v := range resTS {
			if !v {
				log.Errorf("metrics query has no results for query type: %s", k)
				return fmt.Errorf("metrics query has no results for query type: %s", k)
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

func init() {
	rootCmd.PersistentFlags().StringP("dest", "d", "", "Server URL.")
	rootCmd.PersistentFlags().StringP("indexPrefix", "i", "ind", "index prefix")
	rootCmd.PersistentFlags().StringP("indexName", "a", "", "index name")
	rootCmd.PersistentFlags().StringP("bearerToken", "r", "", "Bearer token")

	ingestCmd.PersistentFlags().IntP("processCount", "p", 1, "Number of parallel process to ingest data from.")
	ingestCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of events to send")
	ingestCmd.PersistentFlags().BoolP("continuous", "c", false, "Continous ingestion will ingore -t and will constantly send events as fast as possible")
	ingestCmd.PersistentFlags().IntP("batchSize", "b", 100, "Batch size")

	esBulkCmd.Flags().BoolP("timestamp", "s", false, "Add timestamp in payload")
	esBulkCmd.PersistentFlags().IntP("numIndices", "n", 1, "number of indices to ingest to")
	esBulkCmd.PersistentFlags().StringP("generator", "g", "dynamic-user", "type of generator to use. Options=[static,dynamic-user,file]. If file is selected, -x/--filePath must be specified")
	esBulkCmd.PersistentFlags().StringP("filePath", "x", "", "path to json file to use as logs")

	metricsIngestCmd.PersistentFlags().IntP("metrics", "m", 1_000, "Number of different metric names to send")

	queryCmd.PersistentFlags().IntP("numIterations", "n", 10, "number of times to run entire query suite")
	queryCmd.PersistentFlags().BoolP("verbose", "v", false, "Verbose querying will output raw docs returned by queries")
	queryCmd.PersistentFlags().BoolP("continuous", "c", false, "Continuous querying will ignore -c and -v and will continuously send queries to the destination")
	queryCmd.PersistentFlags().BoolP("validateMetricsOutput", "y", false, "check if metric querries return any results")
	queryCmd.PersistentFlags().StringP("filePath", "f", "", "filepath to csv file to use to run queries from")
	queryCmd.PersistentFlags().BoolP("randomQueries", "", false, "generate random queries")

	queryCmd.AddCommand(esQueryCmd)
	queryCmd.AddCommand(metricsQueryCmd)

	ingestCmd.AddCommand(esBulkCmd)
	ingestCmd.AddCommand(metricsIngestCmd)
	traceCmd.PersistentFlags().StringP("filePrefix", "f", "", "Name of file to output to")
	traceCmd.PersistentFlags().IntP("totalEvents", "t", 1000000, "Total number of traces to generate")
	traceCmd.Flags().IntP("maxSpans", "s", 100, "max number of spans in a single trace")

	rootCmd.AddCommand(ingestCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(traceCmd)
}
