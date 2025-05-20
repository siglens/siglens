package writer

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	vtable "github.com/siglens/siglens/pkg/virtualtable"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

func ProcessPutPostSingleDocRequest(ctx *fasthttp.RequestCtx, updateArg bool, myid int64) {
	r := bytes.NewReader(ctx.PostBody())
	indexNameIn := utils.ExtractParamAsString(ctx.UserValue("indexName"))
	tsNow := utils.GetCurrentTimeInMs()
	tsKey := config.GetTimeStampKey()

	idInUrl := utils.ExtractParamAsString(ctx.UserValue("_id"))
	idVal, err := url.QueryUnescape(idInUrl)
	if err != nil {
		log.Errorf("ProcessPutPostSingleDocRequest: could not decode idVal=%v, err=%v", idInUrl, err)
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		utils.SetBadMsg(ctx, "")
		return
	}

	docTypeInUrl := utils.ExtractParamAsString(ctx.UserValue("docType"))
	docType, err := url.QueryUnescape(docTypeInUrl)
	if err != nil {
		log.Errorf("ProcessPutPostSingleDocRequest: could not decode docTypeInUrl=%v, err=%v", docTypeInUrl, err)
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		utils.SetBadMsg(ctx, "")
		return
	}

	refreshArg := string(ctx.QueryArgs().Peek("refresh"))
	var flush bool
	if refreshArg == "" {
		flush = false
	} else {
		flush = true
	}

	var isKibanaReq bool
	if strings.Contains(indexNameIn, ".kibana") {
		flush = true
		isKibanaReq = true
	} else {
		isKibanaReq = false
	}

	log.Debugf("ProcessPutPostSingleDocRequest: got doc with index %s and id %s, flush=%v", indexNameIn, idVal, flush)

	request := make(map[string]interface{})
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err = decoder.Decode(&request)
	if err != nil {
		log.Errorf("ProcessPutPostSingleDocRequest: error un-marshalling JSON: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}

	if indexNameIn == "" {
		log.Error("ProcessPutPostSingleDocRequest: error processing request: IndexName is a required parameter.")
		utils.SetBadMsg(ctx, "")
		return
	} else if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Infof("ProcessPutPostSingleDocRequest: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)
		body := string(ctx.PostBody())
		err := vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			log.Errorf("ProcessPutPostSingleDocRequest: Failed to add virtual table for indexName=%v, err=%v", indexNameIn, err)
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			utils.SetBadMsg(ctx, "")
			return
		}
		err = vtable.AddMappingFromADoc(&indexNameIn, &body, myid)
		if err != nil {
			log.Errorf("ProcessPutPostSingleDocRequest: Failed to add mapping from a doc for indexName=%v, err=%v", indexNameIn, err)
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			utils.SetBadMsg(ctx, "")
			return
		}
	}

	if idVal == "" {
		idVal = uuid.New().String()
	}

	request["_id"] = idVal
	if docType != "" {
		request["_type"] = docType
	}
	if len(request) > sutils.MAX_RECORD_SIZE {
		var httpResp utils.HttpServerResponse
		ctx.SetStatusCode(fasthttp.StatusRequestEntityTooLarge)
		httpResp.Message = "Request entity too large"
		httpResp.StatusCode = fasthttp.StatusRequestEntityTooLarge
		utils.WriteResponse(ctx, httpResp)
		return
	}
	localIndexMap := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	idxToStreamIdCache := make(map[string]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer func() {
		segwriter.ReleasePLEs(pleArray)
	}()

	indexNameConverted := AddAndGetRealIndexName(indexNameIn, localIndexMap, myid)
	if isKibanaReq {
		if hook := hooks.GlobalHooks.KibanaIngestSingleDocHook; hook != nil {
			err := hook(ctx, request, indexNameConverted, updateArg, idVal, tsNow, myid)
			if err != nil {
				utils.SendError(ctx, "Failed to process kibana ingest request", "", err)
				return
			}
		} else {
			utils.SendError(ctx, "Kibana is not supported", "", nil)
		}

		return
	} else {
		rawData, _ := json.Marshal(request)
		ple, err := segwriter.GetNewPLE(rawData, tsNow, indexNameConverted, &tsKey, jsParsingStackbuf[:])
		if err != nil {
			log.Errorf("ProcessPutPostSingleDocRequest: failed in GetNewPLE , rawData: %v err: %v", string(rawData), err)
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			utils.SetBadMsg(ctx, "")
			return
		}
		pleArray = append(pleArray, ple)
		err = ProcessIndexRequestPle(tsNow, indexNameConverted, flush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
		if err != nil {
			log.Errorf("ProcessPutPostSingleDocRequest: Failed to ingest request, err: %v", err)
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			utils.SetBadMsg(ctx, "")
			return
		}
		SendIndexSuccess(ctx, request, updateArg)
	}
}

func SendIndexSuccess(ctx *fasthttp.RequestCtx, request map[string]interface{}, updateArg bool) {

	var docResp utils.DocIndexedResponse

	if val, pres := request["_type"]; pres {
		docResp.Type = val.(string)
	}

	if val, pres := request["_index"]; pres {
		docResp.Index = val.(string)
	}

	if val, pres := request["_id"]; pres {
		docResp.Id = val.(string)
	}

	docResp.Version = 1
	docResp.SequenceNumber = 1
	//todo this val can be "created" or "updated"
	if updateArg {
		docResp.Result = "updated"
	} else {
		docResp.Result = "created"
	}
	docResp.PrimaryTerm = 2

	shards := make(map[string]interface{})
	shards["total"] = 1
	shards["successful"] = 1
	shards["skipped"] = 0
	shards["failed"] = 0
	docResp.Shards = shards

	var subField utils.DocIndexedResponseSubFieldGet
	subField.SequenceNumber = 1
	subField.PrimaryTerm = 2
	subField.Found = true
	subField.Source = make(map[string]interface{})
	docResp.Get = subField

	utils.WriteJsonResponse(ctx, docResp)
}
