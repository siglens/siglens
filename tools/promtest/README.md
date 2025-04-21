# Prometheus Tester
This is a tool to quickly check what Prometheus gives for query given specific metric datapoints.

## Usage
1. Update server.py with the data you want to ingest. Use unix epochs in milliseconds for the timestamps.
2. `make restart`
3. Wait a few seconds for Prometheus to scrape and flush the data.
4. Run your query. For example:
   ```
   curl -G "http://localhost:9090/api/v1/query_range" \
     --data-urlencode "query=temperature_gauge{sensor=\"A\"}" \
     --data-urlencode "start=1700000000" \
     --data-urlencode "end=1700000300" \
     --data-urlencode "step=10s"
   ```

When you're done, run `make stop`.

## Remove Old Data
Just run `make restart`.
Do not run `docker compose restart` as it will not remove the old data.
