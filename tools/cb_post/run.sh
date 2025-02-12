#!/bin/bash

TRIES=3

QUERY_NUM=0

cat 'queries.spl' | while read -r QUERYTXT; do

  echo -n "["

  for i in $(seq 1 $TRIES); do
    if [[ $QUERYTXT != "null" ]]; then
      JSON="{
              \"state\": \"query\",
              \"searchText\": \"$QUERYTXT\",
              \"startEpoch\": \"now-9000d\",
              \"endEpoch\": \"now\",
              \"indexName\": \"hits\",
              \"from\": 0,
              \"queryLanguage\": \"Splunk QL\"
            }"

      # start external timer
      START=$(date +%s.%N)

      # Run Query directly through search API
      SIG_RSP=$(curl -s -k -X POST "http://localhost:5122/api/search" -H 'Content-Type: application/json' -d"$JSON")

      # calculate timing outside of SigLens
      END=$(date +%s.%N)
      RES=$(echo "$END - $START" | bc -l | xargs printf "%.3f")

      # if SigLens returned an error, print null
      [[ "$(jq 'has("error")' <<<$SIG_RSP)" == "true" ]] && echo -n "null" || echo -n "$RES"
      [[ "$i" != $TRIES ]] && echo -n ", "

      if [[ "$(jq 'has("error")' <<<"$SIG_RSP")" == "true" ]]; then
        echo -e "\n\nSigLens got error for query: $QUERYTXT"
        echo $SIG_RSP
        echo -e "\n"
        FINAL_TIME="null"
      else
        FINAL_TIME="$RES"
      fi
      # output to result file
      echo "${QUERY_NUM},${i},${FINAL_TIME}" >>result.csv
    else
      # Queries that are not supported write null for them
      echo -n "null, "
      echo "${QUERY_NUM},${i},null" >>result.csv
    fi
  done

  echo "],"
  QUERY_NUM=$((QUERY_NUM + 1))

done
