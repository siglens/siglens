/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package query

import (
	"fmt"
	"time"

	"github.com/nqd/flat"
	"github.com/siglens/siglens/pkg/scroll"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/valyala/fasthttp"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func checkScrollSize(offset uint64, size uint64, total uint64) uint64 {
	if offset+size >= total {
		size = total - offset
	}
	return size
}

func GetQueryResponseJsonScroll(indexName string, queryStart time.Time, sizeLimit uint64,
	scrollRecord *scroll.Scroll, qid uint64) utils.HttpServerESResponseScroll {
	var httpRespOuter utils.HttpServerESResponseOuter
	var httpResp utils.HttpServerESResponseScroll
	var subset []utils.Hits
	if scroll.IsScrollIdValid(scrollRecord.Scroll_id) {
		resultLen := scrollRecord.Results.Hits.GetHits()
		scrollRecord.Size = checkScrollSize(scrollRecord.Offset, scrollRecord.Size, resultLen)
		subset = scrollRecord.Results.Hits.Hits[scrollRecord.Offset : scrollRecord.Offset+scrollRecord.Size]
		scrollRecord.Offset = scrollRecord.Offset + scrollRecord.Size
		scroll.SetScrollRecord(scrollRecord.Scroll_id, scrollRecord)
		err := scrollRecord.FlushScrollContextToFile()
		if err != nil {
			log.Errorf("qid=%d, GetQueryResponseJsonScroll: error flushing scroll result for id %+v Err: %v", qid, scrollRecord.Scroll_id, err)
		}
		if len(subset) > 0 {
			err := scrollRecord.WriteScrollResultToFile()
			if err != nil {
				log.Errorf("qid=%d, GetQueryResponseJsonScroll: error writing scroll result %v", qid, err)
			}
		}
		httpResp.Hits.Hits = subset
		httpResp.Took = time.Since(queryStart).Milliseconds()
		log.Infof("qid=%d, Scroll Query Took %+v ms", qid, httpRespOuter.Took)
		httpResp.Hits.Total = scrollRecord.Results.Hits.Total
		httpResp.Timed_out = false
		httpResp.StatusCode = 200
		httpResp.Scroll_id = scrollRecord.Scroll_id
	} else {
		httpResp.StatusCode = fasthttp.StatusBadRequest
		httpResp.Hits.Hits = []utils.Hits{}
		log.Errorf("qid=%d, Scroll Timeout %v : Invalid Search context", qid, scrollRecord.Scroll_id)
	}
	return httpResp

}

func GetQueryResponseJson(nodeResult *structs.NodeResult, indexName string, queryStart time.Time, sizeLimit uint64, qid uint64, aggs *structs.QueryAggregators) utils.HttpServerESResponseOuter {
	var httpRespOuter utils.HttpServerESResponseOuter
	var httpResp utils.HttpServerESResponse

	// aggs exist, so just return aggregations instead of all results
	httpRespOuter.Aggs = make(map[string]utils.BucketWrapper)
	for aggName, aggRes := range nodeResult.Histogram {
		allBuckets := make([]map[string]interface{}, len(aggRes.Results))
		for idx, hist := range aggRes.Results {
			res := make(map[string]interface{})
			res["key"] = hist.BucketKey
			res["doc_count"] = hist.ElemCount
			if aggRes.IsDateHistogram {
				res["key_as_string"] = fmt.Sprintf("%v", hist.BucketKey)
			}
			for name, value := range hist.StatRes {
				res[name] = utils.StatResponse{
					Value: value.CVal,
				}
			}

			allBuckets[idx] = res
		}
		httpRespOuter.Aggs[aggName] = utils.BucketWrapper{Bucket: allBuckets}
	}

	if sizeLimit == 0 || len(nodeResult.AllRecords) == 0 {
		httpResp.Hits = make([]utils.Hits, 0)
	} else {
		var _id string
		allJsons, _, err := record.GetJsonFromAllRrc(nodeResult.AllRecords, true, qid, nodeResult.SegEncToKey, aggs)
		if err != nil {
			log.Errorf("qid=%d, GetQueryResponseJson: failed to get allrecords from rrc, err=%v", qid, err)
			return httpRespOuter
		}
		for _, jsonSource := range allJsons {
			if val, pres := jsonSource["_id"]; pres {
				_id = val.(string)
			} else {
				_id = ""
			}
			var idxToPut string
			if val, pres := jsonSource["_index"]; pres {
				idxToPut = val.(string)
			} else {
				idxToPut = indexName
			}
			var docTypeToPut string
			if val, pres := jsonSource["_type"]; pres {
				docTypeToPut = val.(string)
			} else {
				docTypeToPut = "unknown"
			}

			finalSrc, err := flat.Unflatten(jsonSource, nil)
			if err != nil {
				log.Errorf("qid=%d, GetQueryResponseJson: Failed to unflatten, src=[%v], err=%v", qid, jsonSource, err)
				return httpRespOuter
			}

			jsonMap := utils.Hits{Index: idxToPut, Type: docTypeToPut, Id: _id, Version: 1, Score: 1, Source: finalSrc}

			httpResp.Hits = append(httpResp.Hits, jsonMap)
		}
	}

	httpResp.Total = convertQueryCountToESResponse(nodeResult.TotalResults)
	httpResp.Max_score = 0
	httpRespOuter.Hits = httpResp
	httpRespOuter.Took = time.Since(queryStart).Milliseconds()
	log.Infof("qid=%d, Query Took %+v ms", qid, httpRespOuter.Took)

	httpRespOuter.Timed_out = false
	httpRespOuter.StatusCode = 200

	shards := make(map[string]interface{})
	shards["total"] = 1
	shards["successful"] = 1
	shards["skipped"] = 0
	shards["failed"] = 0

	httpRespOuter.Shards = shards
	return httpRespOuter
}

func convertQueryCountToESResponse(qc *structs.QueryCount) interface{} {
	if qc == nil {
		return 0
	}

	if !qc.EarlyExit {
		return qc.TotalCount
	}

	return utils.HitsCount{Value: qc.TotalCount, Relation: qc.Op.ToString()}
}
