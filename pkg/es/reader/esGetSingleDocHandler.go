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

package reader

import (
	"net/url"
	"strings"
	"time"

	"github.com/nqd/flat"
	"github.com/siglens/siglens/pkg/es/query"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessSingleDocGetRequest(ctx *fasthttp.RequestCtx, myid uint64) {

	var response = utils.NewSingleESResponse()

	queryStart := time.Now()

	idInUrl := utils.ExtractParamAsString(ctx.UserValue("idVal"))

	idVal, err := url.QueryUnescape(idInUrl)
	if err != nil {
		log.Errorf("ProcessSingleDocGetRequest: could not decode idVal=%v, err=%v", idInUrl, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	indexNameIn := utils.ExtractParamAsString(ctx.UserValue("indexName"))
	docTypeVal := utils.ExtractParamAsString(ctx.UserValue("docType"))

	// todo During search, if search request is on a alias
	// we only search the first mapped real indexname, however we should search
	// in multiple indexnames, if the alias was pointing to multiple of them
	var indexNameConverted string
	if pres, idxName := vtable.IsAlias(indexNameIn, myid); pres {
		indexNameConverted = idxName
	} else {
		indexNameConverted = indexNameIn
	}

	qid := rutils.GetNextQid()
	log.Infof("qid=%d, ProcessSingleDocGetRequest: indexNameIn=[%v], indexNameConverted=[%v], idVal=[%v]",
		qid, indexNameIn, indexNameConverted, idVal)

	response.Id = idVal
	response.Index = indexNameIn
	response.Type = docTypeVal

	var respSrc map[string]interface{}
	var simpleNode *ASTNode

	isKibanaIndex := strings.Contains(indexNameIn, ".kibana")
	// TODO: get error from this function
	simpleNode = query.CreateSingleDocReqASTNode("_id", idVal, isKibanaIndex, qid)

	segment.LogASTNode("esGetSingleDocHandler", simpleNode, qid)
	sizeLimit := uint64(1)
	qc := structs.InitQueryContext(indexNameConverted, sizeLimit, 0, myid, true)
	segment.LogQueryContext(qc, qid)
	result := segment.ExecuteQuery(simpleNode, &QueryAggregators{}, qid, qc)

	if result == nil {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		utils.WriteJsonResponse(ctx, response)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("Content-Type", "application/json")
	queryResult := query.GetQueryResponseJson(result, indexNameConverted, queryStart, sizeLimit, qid, &QueryAggregators{})

	if queryResult.Hits.GetHits() == 0 {
		utils.WriteJsonResponse(ctx, response)
		return
	}
	// TODO: fix hard coded fields
	response.Found = true
	respSrc = queryResult.Hits.Hits[0].Source

	finalSrc, err := flat.Unflatten(respSrc, nil)
	if err != nil {
		log.Infof("qid=%d, ProcessSingleDocGetRequest: Failed to unflatten, src=[%v], err=%v", qid, respSrc, err)
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		utils.WriteJsonResponse(ctx, response)
		return
	}
	response.Source = finalSrc
	utils.WriteJsonResponse(ctx, response)
}
