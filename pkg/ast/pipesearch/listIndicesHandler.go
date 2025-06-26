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

package pipesearch

import (
	"sort"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type IndexInfo struct {
	Name string `json:"index"`
}

type AllIndicesInfoResponse []*IndexInfo

type AllColumnNamesResponse []string

func ListIndicesHandler(ctx *fasthttp.RequestCtx, orgId int64) {
	var httpResp AllIndicesInfoResponse
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("Content-Type", "application/json")

	var allIndexNames []string
	var indexNamesMap map[string]bool
	if hooks.GlobalHooks.AddMultiNodeIndexHook != nil {
		allIndexNames = hooks.GlobalHooks.AddMultiNodeIndexHook(orgId)
	}
	if len(allIndexNames) != 0 {
		indexNamesMap = make(map[string]bool)
		for _, name := range allIndexNames {
			indexNamesMap[name] = true
		}
		allIndexNamesList := vtable.ExpandAndReturnIndexNames("*", orgId, false, ctx)
		for _, name := range allIndexNamesList {
			if _, exists := indexNamesMap[name]; !exists {
				indexNamesMap[name] = true
				allIndexNames = append(allIndexNames, name)
			}
		}
	} else {
		allIndexNames = vtable.ExpandAndReturnIndexNames("*", orgId, false, ctx)
	}
	sort.Strings(allIndexNames)

	if len(allIndexNames) == 0 {
		noTables := []*IndexInfo{}
		httpResp = noTables
		utils.WriteJsonResponse(ctx, httpResp)
		return
	}
	var listIndices []*IndexInfo

	for _, indexName := range allIndexNames {
		IndexInfo := IndexInfo{}
		if indexName == "" || indexName == "*" {
			log.Debugf("ListIndicesHandler: one of empty/wildcard indexName=%v", indexName)
			continue
		}
		IndexInfo.Name = indexName
		listIndices = append(listIndices, &IndexInfo)
		httpResp = listIndices
	}

	utils.WriteJsonResponse(ctx, httpResp)
}

type Version struct {
	Number        string `json:"number"`
	BuildFlavor   string `json:"build_flavor"`
	BuildType     string `json:"build_type"`
	BuildHash     string `json:"build_hash"`
	BuildDate     string `json:"build_date"`
	BuildSnap     bool   `json:"build_snapshot"`
	LuceneVersion string `json:"lucene_version"`
	MWCV          string `json:"minimum_wire_compatibility_version"`
	MICV          string `json:"minimum_index_compatibility_version"`
}

type ESResponse struct {
	Name        string      `json:"name"`
	ClusterName string      `json:"cluster_name"`
	ClusterUUID string      `json:"cluster_uuid"`
	Version     interface{} `json:"version"`
	TagLine     string      `json:"tagline"`
}

// returns cluster details
func SendClusterDetails(ctx *fasthttp.RequestCtx) {
	// hard coding response for superset
	versionRes := Version{
		Number:        "7.17.10",
		BuildFlavor:   "default",
		BuildType:     "deb",
		BuildHash:     "fecd68e3150eda0c307ab9a9d7557f5d5fd71349",
		BuildDate:     "2023-04-23T05:33:18.138275597Z",
		BuildSnap:     false,
		LuceneVersion: "8.11.1",
		MWCV:          "6.8.0",
		MICV:          "6.0.0-beta1",
	}

	esRes := ESResponse{
		Name:        "test",
		ClusterName: "elasticsearch",
		ClusterUUID: "mQAvDxlxTkiDGTwojQ_aOg",
		Version:     versionRes,
		TagLine:     "Hey Superset",
	}

	utils.WriteJsonResponse(ctx, esRes)
}

func ListColumnNamesHandler(ctx *fasthttp.RequestCtx, orgId int64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ListColumnNamesHandler: received empty search request body")
		utils.SetBadMsg(ctx, "")
		return
	}

	readJSON, err := utils.DecodeJsonToMap(rawJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("ListColumnNamesHandler: could not write error message, err: %v", err)
		}
		log.Errorf("ListColumnNamesHandler: failed to decode search request body! err: %+v", err)
		return
	}

	nowTs := utils.GetCurrentTimeInMs()
	_, startEpoch, endEpoch, _, indexNameIn, _, _, _ := ParseSearchBody(readJSON, nowTs)

	// todo get indexnames from multinode
	allIndexNames := vtable.ExpandAndReturnIndexNames(indexNameIn, orgId, false, ctx)

	tRange := new(dtu.TimeRange)
	tRange.StartEpochMs = startEpoch
	tRange.EndEpochMs = endEpoch

	resAllColumns := make(map[string]struct{})
	metadata.CollectColumnsForTheIndexesByTimeRange(tRange, allIndexNames, orgId, resAllColumns)
	writer.CollectUnrotatedColumnsForTheIndexesByTimeRange(tRange, allIndexNames, orgId,
		resAllColumns)

	allCnamesResp := utils.GetKeysOfMap(resAllColumns)

	sort.Strings(allCnamesResp)

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("Content-Type", "application/json")
	utils.WriteJsonResponse(ctx, allCnamesResp)
}
