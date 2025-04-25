# SigScalr Client

## Ingest

### ES Bulk
To send ingestion traffic to a server using ES Bulk API:
```bash
go run main.go ingest esbulk -t 10_000 -d http://localhost:8081/elastic -p 2
```
Options:
```
  -b, --batchSize int        Batch size (default 100)
  -d, --dest string          Destination URL. Client will append /_bulk
  -g, --generator string     type of generator to use. Options=[static,dynamic-user,file,benchmark]. If file is selected, -x/--filePath must be specified (default "static")
  
  -x, --filePath string      path to json file containing loglines to send to server
  -h, --help                 help for ingest
  -i, --indexPrefix string   Index prefix to ingest (default "ind")
  -r, --bearerToken string   Bearer token of your org to ingest (default "")
  -n, --numIndices int       number of indices to ingest to (default 1)
  -p, --processCount int     Number of parallel process to ingest data from. (default 1)
  -t, --totalEvents int      Total number of events to send (default 1000000)
  -s, --timestamp            If set, adds "timestamp" to the static/dynamic generators
  -e, --eventsPerDay uint    Number of events to ingest per day. If set, the ingestion mode will be assumed to be continuous.

  -c  continuous             If true, ignores -t and will continuously send docs to the destination
  -o  --outputFile           filepath to output the query response Time results in csv format.
  -t  --runResponseTime      If true, then the queries will be run and the response time will be recorded in the given/default outputFile in CSV Format.
      --enableVariableNumColumns bool  Set this to true to generate variable number of columns per record. Each record will have a random number of columns between minColumns and maxColumns
      --maxColumns  int      Maximum number of Columns to generate. By Default this value is set to 100.
      --minColumns int       Minimum number of Columns to generate. Default value is 0. If 0, it will be set to maxColumns value.
```

Different Types of Readers:

1. Static: Sends the same payload over and over
2. Dynamic User: Randomly Generates user events. These random events are generated using [gofakeit](github.com/brianvoe/gofakeit/v6).
3. File: Reads a file line by line. Expects each line is a new json. Will loop over file if necessary

To Ingest 200 columns per record:

```bash
go run main.go ingest esbulk -n 1 -g benchmark -d http://localhost:8081/elastic -t 1000 -p 1 --enableVariableNumColumns true --maxColumns 200
```

To Ingest variable number of Columns between 50 to 200. Each record will have columns count between 50 and 200.

```bash
go run main.go ingest esbulk -n 1 -g benchmark -d http://localhost:8081/elastic -t 1000 -p 1 --enableVariableNumColumns true --maxColumns 200 --minColumns 50
```


### OTSDB
To send ingestion traffic to a server using OTSDB:
```bash
go run main.go ingest metrics -d http://localhost:8081/otsdb -t 10_000  -m 5 -p 1
```
Options:
```
  -m, --metrics int   Number of different metric names to send (default 1000)
  -r, --bearerToken string   Bearer token of your org to ingest (default "")
  -b, --batchSize int        Batch size (default 100)
  -d, --dest string          Server URL.
  -p, --processCount int     Number of parallel process to ingest data from. (default 1)
  -t, --totalEvents int      Total number of events to send (default 1000000)
  -u, --uniqueness int       Cardinality (uniqueness) of the data (default 2000000)
  -e, --eventsPerDay uint    Number of events to ingest per day. If set, the ingestion mode will be assumed to be continuous.
```

## Query

### OTSDB
To send queries using OTSDB and measure responses to a server:
```bash
go run main.go otsdb -d http://localhost:5122/otsdb -v
```


Options:
```
-d, --dest string          Destination URL. Client will append /api/query
-n, --numIterations int    Number of iterations to send query suite (default 10)
-r, --bearerToken string   Bearer token of your org to ingest (default "")
-v  verbose                Output hits and elapsed time for each query
-c  continuous             If true, ignores -n and -v and will continuously send queries to the destination and will log results
```

### ESDSL
To send queries using ESDSL and measure responses to a server:
```bash
go run main.go query esbulk -d http://localhost:5122/elastic -v
```


Options:
```
-d, --dest string          Destination URL. Client will append /{indexPrefix}*/_search
-i, --indexPrefix string   Index prefix to search (default "ind")
-r, --bearerToken string   Bearer token of your org to ingest (default "")
-n, --numIterations int    Number of iterations to send query suite (default 10)
-f, --filePath string      path to csv file containing query suite to send to server. Expects CSV of with [search text, startTime, endTime, indexName, evaluation type, relation, count, queryLanguage] in each row
    --randomQueries bool   Generate random queries (default false)
-v  verbose                Output hits and elapsed time for each query
-c  continuous             If true, ignores -n and -v and will continuously send queries to the destination and will log results
-l, --lookups []string     List of lookup files to migrate to the `data/lookups` folder before executing the queries. Enter the paths of these files.
```

#### Notes
When using a CSV file, the `evaluation type` parameter should be either:
 - `total` to test the total number of returned rows
 - A colon-separated list of strings to test the value returned by an aggregation function. The first element should be `group`, the second should be the aggregation to test, and the rest specify the keys to test for.
For example, a valid CSV row is:
```
"min(latency) groupby city, http_method",now-1d,now,*,group:min(latency):Boston:POST,eq,5479,Pipe QL
```
To test an aggregation that doesn't have a groupby clause, use something like the following (notice the `*` after the last `:`):
```
min(latency),now-1d,now,*,group:min(latency):*,eq,110,Pipe QL
```

## Generating traces
To generate synthetic traces: 
```bash
go run main.go traces -f test_traces -t 1000 -s 10
```

Options:
```
-f, --filePrefix string        prefix path for spans and services to be generated 
-t, --totalEvents int          number of total traces to generate
-s, --maxSpans int             number of max spans for each trace
```

To send ingestion traffic to a server using ES Bulk API:
```bash
go run main.go ingest esbulk -t 1000 -d http://localhost:8081/elastic -g file -x {filePrefix}_services.json --indexName jaeger-service-YYYY-MM-DD
go run main.go ingest esbulk -t 1000 -d http://localhost:8081/elastic -g file -x {filePrefix}_spans.json --indexName jaeger-span-YYYY-MM-DD
```

Options:
```
-t, --totalEvents int          number of total traces to ingest (default 1000000)
-d, --dest string              Destination URL. Client will append /_bulk
-g, --generator string         type of generator to use. Options=[static,dynamic-user,file,benchmark]. Select file and pass the path to the above two created files using -f/--filePath
-x, --filePath string          path to json file to output traces and services to
-n, --numIndices int           number of indices to ingest to (default 1) 
-i, --indexName string         Index name to ingest to (default "ind"), for tracing it is important to use this argument in the same format
```

## Alerts

**Run E2E Tests For Alerting**

```bash
go run main.go alerts e2e -d http://localhost:5122
```

Options:

```
-d, --dest string           The Host of the query server
```

## Utils

To convert a TSV to a JSON file that can be ingested via `-f file`:
```bash
go run cmd/utils/converter.go --input {input file name} --output {output file name}
```

## Functional Test
To run functional test use the following command
```
go run main.go functional -d http://localhost:8081/elastic -f functionalQueries/functionalQueries.yml -q localhost:5122
```

Options:
```
-d, --dest string              Complete destination URL of the ingest server.
-q, --queryDest string         Query server URL in the form IP:PORT.
-f, --queriesToRunFile string  Path to the file containing paths of functional query files to be tested.
-l, --longer bool              Run a longer functional test.
-i, --doIngest bool            Perform ingestion for functional Test (default true)
-u, --doQuery bool             Perform query for functional Test (default true)
```

To run longer functional test use the following command
```
go run main.go functional -d http://localhost:8081/elastic -f longerfunctionalQueries/longerfunctionalQueries.yml -q localhost:5122 -l true
```


## Performance Test
To run the performance test use the following command
```
go run main.go performance -d http://localhost:8081/elastic -q localhost:5122
```

Options:
```
-d, --dest string              Complete destination URL of the ingest server.
-q, --queryDest string         Query server URL in the form IP:PORT.
```

## Concurrent Query Test
To run the concurrent queries test use the following command
```
go run main.go concurrentQueries -d localhost:5122 -q "*" -c 3 -i 2
```

Options:
```
-d, --dest                   string   Query server URL in the form IP:PORT.
-q, --queryText              string   Query to run.
-c, --numOfConcurrentQueries int      Number of concurrent queries to run.
-i, --iterations             int      Number of iterations for this test
```

## Longevity Test
To run the longevity test use the following command
```
go run main.go longevity -d http://localhost:8081/elastic -q http://localhost:5122/api/search
```

Options:
```
-d, --dest string              Complete destination URL of the ingest server.
-q, --queryDest string         Query server URL in the form IP:PORT.
```
