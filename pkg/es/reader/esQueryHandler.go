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

package reader

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/es/query"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/scroll"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var ScrollLimit uint64 = 10000

func ProcessSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	var httpResp utils.HttpServerESResponseOuter
	var httpRespScroll utils.HttpServerESResponseScroll
	queryStart := time.Now()

	queryJson := ProcessHttpGetRequest(ctx)
	queryArgs := ctx.QueryArgs()
	scrollTimeout := queryArgs.Peek("scroll")
	getTotalHits, err := strconv.ParseBool(string(queryArgs.Peek("rest_total_hits_as_int")))
	if err != nil {
		getTotalHits = false
	}

	indexNameUrl := utils.ExtractParamAsString(ctx.UserValue("indexName"))
	indexNameIn, err := url.QueryUnescape(indexNameUrl)
	if err != nil {
		log.Errorf("ProcessSearchRequest: could not decode indexNameUrl=%v, err=%v", indexNameUrl, err)
		var httpResp utils.HttpServerResponse
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = "Bad Request"
		httpResp.StatusCode = fasthttp.StatusBadRequest
		utils.WriteResponse(ctx, httpResp)
		return
	}

	if indexNameIn == "" {
		log.Infof("ProcessSearchRequest: No index name provided. Retrieving all index names")
		indexNameIn = "*"
	}

	ti := structs.InitTableInfo(indexNameIn, myid, true)
	isJaegerQuery := false
	for _, indexName := range ti.GetQueryTables() {
		if strings.HasPrefix(indexName, "jaeger-") {
			isJaegerQuery = true
		} else {
			isJaegerQuery = false
			break
		}
	}

	qid := rutils.GetNextQid()
	log.Infof("qid=%v, esQueryHandler: tableInfo=[%v], queryJson=[%v] scroll = [%v]",
		qid, ti.String(), string(queryJson), string(scrollTimeout))

	requestURI := ctx.URI().String()
	var simpleNode *structs.ASTNode
	var aggs *structs.QueryAggregators
	var sizeLimit uint64
	var scrollRecord *scroll.Scroll
	if strings.Contains(requestURI, "_opendistro") {
		simpleNode, aggs, sizeLimit, scrollRecord, err = query.ParseOpenDistroRequest(queryJson, qid, isJaegerQuery, string(scrollTimeout))
	} else {
		simpleNode, aggs, sizeLimit, scrollRecord, err = query.ParseRequest(queryJson, qid, isJaegerQuery, string(scrollTimeout))
	}
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, esQueryHandler: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, esQueryHandler: Error parsing query err=%+v", qid, err)
		return
	}

	aggs.EarlyExit = !getTotalHits // if we should get total hits, don't early exit
	if specialQuery, aggName := isAllIndexAggregationQuery(simpleNode, aggs, qid); specialQuery {
		log.Infof("qid=%d, ProcessSearchRequest: Processing special query for only index name aggregations.", qid)
		res := getIndexNameAggOnly(aggName, myid)
		httpResp = query.GetQueryResponseJson(res, indexNameIn, queryStart, sizeLimit, qid, aggs)
		utils.WriteJsonResponse(ctx, httpResp)
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	if simpleNode == nil && aggs == nil && scrollRecord == nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString("Failed to parse query!")
		if err != nil {
			log.Errorf("qid=%v, esQueryHandler: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, esQueryHandler: Failed to parse query, simpleNode=%v, aggs=%v, err=%v", qid, simpleNode, aggs, err)
		return
	}

	if scrollRecord != nil && !scroll.IsScrollIdValid(scrollRecord.Scroll_id) {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString("Scroll Timeout : Invalid Search context")
		if err != nil {
			log.Errorf("qid=%v, esQueryHandler: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, esQueryHandler: Scroll Timeout %v : Invalid Search context", qid, scrollRecord.Scroll_id)
		return
	}

	if simpleNode == nil && scrollRecord == nil {
		// we construct a "match_all" node
		simpleNode, _ = query.GetMatchAllASTNode(qid)
	}
	segment.LogASTNode("ProcessSearchRequest", simpleNode, qid)
	segment.LogQueryAggsNode("ProcessSearchRequest", aggs, qid)
	log.Infof("qid=%v, esQueryHandler: indexNameIn=[%v], queryJson=[%v] scroll = [%v]",
		qid, indexNameIn, string(queryJson), string(scrollTimeout))

	if scrollRecord != nil {
		if !scroll.IsScrollIdValid(scrollRecord.Scroll_id) {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			_, err = ctx.WriteString("Scroll Timeout : Invalid Search context")
			if err != nil {
				log.Errorf("qid=%v, esQueryHandler: could not write error message err=%v", qid, err)
			}
			log.Errorf("qid=%v, esQueryHandler: Scroll Timeout %v : Invalid Search context", qid, scrollRecord.Scroll_id)
			return
		} else {
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.Response.Header.Set("Content-Type", "application/json")
			if scrollRecord.Results == nil {
				qc := structs.InitQueryContextWithTableInfo(ti, ScrollLimit, 0, myid, true)
				segment.LogQueryContext(qc, qid)
				result := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
				httpRespOuter := query.GetQueryResponseJson(result, indexNameIn, queryStart, sizeLimit, qid, aggs)
				scrollRecord.Results = &httpRespOuter
			}
			httpRespScroll = query.GetQueryResponseJsonScroll(indexNameIn, queryStart, sizeLimit, scrollRecord, qid)
			utils.WriteJsonResponse(ctx, httpRespScroll)
		}
	} else {
		qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, myid, true)
		result := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
		httpResp = query.GetQueryResponseJson(result, indexNameIn, queryStart, sizeLimit, qid, aggs)
		utils.WriteJsonResponse(ctx, httpResp)
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessHttpGetRequest(ctx *fasthttp.RequestCtx) []byte {
	var httpResp utils.HttpServerResponse
	queryJson := ctx.PostBody()
	if queryJson == nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = "Bad request"
		httpResp.StatusCode = fasthttp.StatusBadRequest
		utils.WriteResponse(ctx, httpResp)
		return queryJson
	}
	return queryJson
}

/*
Uses microreader & segwriter to get the doc counts per index name

TODO: how does this look in a multi node setting?
Returns NodeResults with doc counts per index aggregation
*/
func getIndexNameAggOnly(aggName string, myid uint64) *structs.NodeResult {

	allVirtualTableNames, err := vtable.GetVirtualTableNames(myid)
	if err != nil {
		return &structs.NodeResult{ErrList: []error{err}}
	}

	totalHits := uint64(0)
	bucketResults := make([]*structs.BucketResult, 0)
	for indexName := range allVirtualTableNames {
		if indexName == "" {
			log.Errorf("getIndexNameAggOnly: skipping an empty index name indexName=%v", indexName)
			continue
		}
		_, eventCount, _ := segwriter.GetVTableCounts(indexName, myid)
		_, unrotatedEventCount, _ := segwriter.GetUnrotatedVTableCounts(indexName, myid)
		totalEventsForIndex := uint64(eventCount) + uint64(unrotatedEventCount)
		totalHits += totalEventsForIndex
		currBucket := &structs.BucketResult{
			ElemCount: totalEventsForIndex,
			BucketKey: indexName,
		}
		bucketResults = append(bucketResults, currBucket)
	}
	aggResult := make(map[string]*structs.AggregationResult)
	aggResult[aggName] = &structs.AggregationResult{
		IsDateHistogram: false,
		Results:         bucketResults,
	}

	return &structs.NodeResult{
		AllRecords:   make([]*segutils.RecordResultContainer, 0),
		Histogram:    aggResult,
		TotalResults: &structs.QueryCount{TotalCount: totalHits, Op: segutils.Equals},
		SegEncToKey:  make(map[uint16]string),
	}
}
