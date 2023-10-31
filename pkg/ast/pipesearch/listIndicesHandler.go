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

package pipesearch

import (
	"sort"

	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type IndexInfo struct {
	Name string `json:"index"`
}

type AllIndicesInfoResponse []*IndexInfo

func ListIndicesHandler(ctx *fasthttp.RequestCtx, myid uint64) {
	var httpResp AllIndicesInfoResponse
	allVirtualTableNames := vtable.ExpandAndReturnIndexNames("*", myid, false)
	sort.Strings(allVirtualTableNames)
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("Content-Type", "application/json")

	if len(allVirtualTableNames) == 0 {
		noTables := []*IndexInfo{}
		httpResp = noTables
		utils.WriteJsonResponse(ctx, httpResp)
		return
	}
	var listIndices []*IndexInfo
	for _, indexName := range allVirtualTableNames {
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
