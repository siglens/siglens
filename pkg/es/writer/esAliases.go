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

package writer

import (
	"bytes"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessGetAlias(ctx *fasthttp.RequestCtx, myid uint64) {
	log.Infof("ProcessGetAlias:")
	aliasName := utils.ExtractParamAsString(ctx.UserValue("aliasName"))

	respbody := make(map[string]interface{})
	pres, indexName := vtable.IsAlias(aliasName, myid)
	if !pres {
		respbody["error"] = fmt.Sprintf("alias [%v] missing", aliasName)
		respbody["status"] = 404
		utils.WriteJsonResponse(ctx, respbody)
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	// if it is a alias, then we have to do reverse lookup and find the index

	emptyMap := make(map[string]interface{})
	abody := make(map[string]interface{})
	curIdxAliases, err := vtable.GetAliases(indexName, myid)
	if err == nil {
		for k := range curIdxAliases {
			abody[k] = emptyMap
		}
	}
	tBody := make(map[string]interface{})
	tBody["aliases"] = abody
	respbody[indexName] = tBody

	utils.WriteJsonResponse(ctx, respbody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetAllAliases(ctx *fasthttp.RequestCtx, myid uint64) {

	respbody := make(map[string]interface{})
	allIndices, err := vtable.GetVirtualTableNames(myid)
	if err != nil {
		log.Errorf("ProcessGetAllAliases: Failed to get aliases for myid=%v, err=%v", myid, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		respbody := make(map[string]interface{})
		respbody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, respbody)
		return
	}
	emptyMap := make(map[string]interface{})

	for idxName := range allIndices {
		abody := make(map[string]interface{})
		curIdxAliases, err := vtable.GetAliases(idxName, myid)
		log.Debugf("ProcessGetAllAliases: idxName=%v, curIdxAliases=%v", idxName, curIdxAliases)
		if err == nil {
			for k := range curIdxAliases {
				log.Debugf("ProcessGetAllAliases: idxName=%v, k=%v", idxName, k)
				abody[k] = emptyMap
			}
		}
		tBody := make(map[string]interface{})
		tBody["aliases"] = abody
		log.Debugf("ProcessGetAllAliases: abody=%v, tBody=%v", abody, tBody)
		respbody[idxName] = tBody
	}

	utils.WriteJsonResponse(ctx, respbody)
	ctx.SetStatusCode(fasthttp.StatusOK)

}

func ProcessGetIndexAlias(ctx *fasthttp.RequestCtx, myid uint64) {

	indexName := utils.ExtractParamAsString(ctx.UserValue("indexName"))

	if indexName == "" {
		log.Errorf("ProcessGetIndexAlias: one of nil indexName=%v", indexName)
		utils.SetBadMsg(ctx, "")
		return
	}
	log.Infof("ProcessGetIndexAlias: indexName=%v", indexName)

	currentAliases, err := vtable.GetAliases(indexName, myid)
	if err != nil {
		log.Errorf("ProcessGetIndexAlias: GetAliases returned err=%v", err)
		utils.SetBadMsg(ctx, "")
		return
	}

	respbody := make(map[string]interface{})
	alvalues := make(map[string]interface{})
	alvalues["aliases"] = currentAliases
	respbody[indexName] = alvalues

	utils.WriteJsonResponse(ctx, respbody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessPutAliasesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	indexName := utils.ExtractParamAsString(ctx.UserValue("indexName"))
	aliasName := utils.ExtractParamAsString(ctx.UserValue("aliasName"))

	if indexName == "" || aliasName == "" {
		log.Errorf("ProcessPutAliasesRequest: one of nil indexName=%v, aliasName=%v", indexName, aliasName)
		utils.SetBadMsg(ctx, "")
		return
	}
	log.Infof("ProcessPutAliasesRequest: add alias request, indexName=%v, aliasName=%v", indexName, aliasName)
	err := vtable.AddAliases(indexName, []string{aliasName}, myid)
	if err != nil {
		log.Errorf("ProcessPutAliasesRequest: failed to add alias, indexName=%v, aliasName=%v, err=%v", indexName, aliasName, err)
		utils.SetBadMsg(ctx, "")
		return
	}

	respbody := make(map[string]interface{})
	respbody["acknowledged"] = true
	utils.WriteJsonResponse(ctx, respbody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

/*
   {
	"actions" : [
		{ "add" : { "index" : "test1", "alias" : "alias1" } }
	]
	}
*/

func ProcessPostAliasesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	r := bytes.NewReader(ctx.PostBody())

	log.Infof("ProcessPostAliasesRequest: body=%v", string(ctx.PostBody()))

	jsonBody := make(map[string]interface{})
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err := decoder.Decode(&jsonBody)

	if err != nil {
		log.Errorf("ProcessPostAliasesRequest: error un-marshalling JSON: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}
	for key, value := range jsonBody {
		switch value.(type) {
		case []interface{}:
			if key == "actions" {
				processActions(ctx, jsonBody[key], myid)
			} else {
				log.Errorf("ProcessPostAliasesRequest: unknown key: %v", key)
				utils.SetBadMsg(ctx, "")
				return
			}
		default:
			log.Errorf("ProcessPostAliasesRequest: unknown type key: %v, value.Type=%T", key, value)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	respbody := make(map[string]interface{})
	respbody["acknowledged"] = true
	utils.WriteJsonResponse(ctx, respbody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

/*
   [{ "add" : { "index" : "test1", "alias" : "alias1" } }]
   OR
   [{"remove": {"index": "test1", "alias" : "alias1"  } }]
*/

func processActions(ctx *fasthttp.RequestCtx, actions interface{}, myid uint64) {

	switch t := actions.(type) {
	case []interface{}:
		for _, value := range t {
			switch t1 := value.(type) {
			case map[string]interface{}:
				for aKey, aValue := range t1 {
					if aKey == "add" {
						parseAddAction(ctx, aValue, myid)
					} else if aKey == "remove" {
						parseRemoveAction(ctx, aValue, myid)
					} else {
						log.Errorf("processActions: unhandled action aKey: %v", aKey)
						utils.SetBadMsg(ctx, "")
						return
					}
				}
			default:
				log.Errorf("processActions: unknown t1.type=%T, t1=%v", t1, t1)
				utils.SetBadMsg(ctx, "")
				return
			}
		}
	default:
		log.Errorf("processActions: unknown actions.(type)  value.type=%T", t)
		utils.SetBadMsg(ctx, "")
		return
	}
}

/*
   { "index" : "test1", "alias" : "alias1" }}
*/

func parseAddAction(ctx *fasthttp.RequestCtx, params interface{}, myid uint64) {
	log.Infof("parseAddAction: add alias request, params=%v", params)
	switch t := params.(type) {
	case map[string]interface{}:
		aliasName := t["alias"]
		if aliasName == nil {
			log.Errorf("parseAddAction: aliasName was nil, params=%v", params)
			utils.SetBadMsg(ctx, "")
			return
		}
		indexName := t["index"]
		indices := t["indices"]
		if indexName == nil && indices == nil {
			log.Errorf("parseAddAction: both indexName and indices was nil, params=%v", params)
			utils.SetBadMsg(ctx, "")
			return
		}
		doAddAliases(ctx, indexName, aliasName, indices, myid)
	default:
		log.Errorf("parseAddAction: unknown params.(type)=%T  params=%v", params, params)
		utils.SetBadMsg(ctx, "")
		return
	}
}

/*
   { "index" : "test1", "alias" : "alias1" }
   OR
   { "indices" : ["test1", "test2"], "alias" : "alias1" }
*/

func doAddAliases(ctx *fasthttp.RequestCtx, indexName interface{}, aliasName interface{}, indices interface{}, myid uint64) {

	log.Infof("doAddAliases: addalias for indexName=%v, aliasName=%v, indices=%v", indexName, aliasName, indices)

	if indexName != nil {
		err := vtable.AddAliases(indexName.(string), []string{aliasName.(string)}, myid)
		if err != nil {
			log.Errorf("doAddAliases: failed to add alias, indexName=%v, aliasName=%v err=%v", indexName.(string), aliasName.(string), err)
			utils.SetBadMsg(ctx, "")
		}
		return
	}

	switch t := indices.(type) {
	case []interface{}:
		for _, iVal := range t {
			err := vtable.AddAliases(iVal.(string), []string{aliasName.(string)}, myid)
			if err != nil {
				log.Errorf("doAddAliases: failed to add alias, indexName=%v, aliasName=%v err=%v", iVal.(string), aliasName.(string), err)
			}
		}
	default:
		log.Errorf("doAddAliases: unknown indices.(type)=%T  indices=%v", indices, indices)
		utils.SetBadMsg(ctx, "")
		return
	}
}

/*

	{ "index" : "test1", "alias" : "alias1" }

*/

func parseRemoveAction(ctx *fasthttp.RequestCtx, params interface{}, myid uint64) {
	log.Infof("parseRemoveAction: remove alias request, params=%v", params)
	switch t := params.(type) {
	case map[string]interface{}:
		aliasName := t["alias"]
		if aliasName == nil {
			log.Errorf("parseRemoveAction: aliasName was nil, params=%v", params)
			utils.SetBadMsg(ctx, "")
			return
		}
		indexName := t["index"]
		if indexName == nil {
			log.Errorf("parseRemoveAction: both indexName was nil, params=%v", params)
			utils.SetBadMsg(ctx, "")
			return
		}
		err := vtable.RemoveAliases(indexName.(string), []string{aliasName.(string)}, myid)
		if err != nil {
			log.Errorf("parseRemoveAction: failed to remove alias, indexName=%v, aliasName=%v err=%v", indexName.(string), aliasName.(string), err)
			utils.SetBadMsg(ctx, "")
		}
		return
	default:
		log.Errorf("parseRemoveAction: unknown params.(type)=%T  params=%v", params, params)
		utils.SetBadMsg(ctx, "")
		return
	}
}

func ProcessIndexAliasExist(ctx *fasthttp.RequestCtx, myid uint64) {

	//get indexName and aliasName
	indexName := utils.ExtractParamAsString(ctx.UserValue("indexName"))
	aliasName := utils.ExtractParamAsString(ctx.UserValue("aliasName"))
	if indexName == "" {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		log.Errorf("ProcessIndexAliasExist : indexName is required")
		ctx.SetContentType(utils.ContentJson)
		return
	}
	alias, _ := vtable.IsAlias(aliasName, myid)

	allIndexNames, _ := vtable.GetVirtualTableNames(myid)

	_, exists := allIndexNames[indexName]
	if exists {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetContentType(utils.ContentJson)
		return
	}
	//for alias
	if aliasName == "" || !alias {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		log.Errorf("ProcessIndexAliasExist : aliasName is required")
		ctx.SetContentType(utils.ContentJson)
	} else {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetContentType(utils.ContentJson)
	}
}
