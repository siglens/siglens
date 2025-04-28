This document outlines the process for running a benchmark on SigLens, a observability platform.

Note about queries:
- SigLens does not support SQL but supports Splunk Query Language (SPL). The SQL queries used by the benchmark have been translated into the splunk query language.
- To ensure the accuracy of the translated Splunk Query Language queries, each SQL query was executed against the same dataset in ClickHouse. The responses from SigLens and ClickHouse were compared, and all results were identical.
- Some of the original queries are not supported and not run by the benchmark. The corresponding results have been recorded as null in `queries.spl` and `results.csv` respectively.
