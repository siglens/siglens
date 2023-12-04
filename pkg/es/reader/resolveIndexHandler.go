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
	"regexp"
	"strings"

	esutils "github.com/siglens/siglens/pkg/es/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/siglens/siglens/pkg/virtualtable"
	"github.com/valyala/fasthttp"

	log "github.com/sirupsen/logrus"
)

var excludedInternalIndices = [...]string{"traces", "red-traces", "service-dependency"}

func IndicesBody(indexName string) esutils.ResolveIndexEntry {
	return esutils.ResolveIndexEntry{Name: indexName, Attributes: []string{"open"}}
}

func ExpandAndReturnIndexNames(indexPattern string, allVirtualTableNames map[string]bool, myid uint64) ([]esutils.ResolveIndexEntry, []esutils.ResolveAliasEntry, error) {
	indicesEntries := []esutils.ResolveIndexEntry{}
	aliasesEntries := []esutils.ResolveAliasEntry{}

	if strings.Contains(indexPattern, "*") {
		startLimiter := "^"
		endLimiter := "$"
		indexPattern = startLimiter + indexPattern + endLimiter
		indexRegExp, err := regexp.Compile(strings.ReplaceAll(indexPattern, "*", `.*`))
		if err != nil {
			log.Infof("ExpandAndReturnIndexNames: Error compiling match: %v", err)
			return indicesEntries, aliasesEntries, err
		}

		for indexName := range allVirtualTableNames {
			if indexRegExp.MatchString(indexName) {
				if isIndexExcluded(indexName) {
					continue
				}
				newEntry := IndicesBody(indexName)
				currentAliases, err := virtualtable.GetAliasesAsArray(indexName, myid)
				if err != nil {
					log.Errorf("ExpandAndReturnIndexNames: GetAliases returned err=%v", err)
					newEntry.Aliases = []string{}
				} else {
					newEntry.Aliases = currentAliases
				}
				indicesEntries = append(indicesEntries, newEntry)
			}
		}

		aliasInfos, _ := virtualtable.GetAllAliasesAsMapArray(myid)
		for aliasName, indexes := range aliasInfos {
			if indexRegExp.MatchString(aliasName) {
				newEntry := esutils.ResolveAliasEntry{Name: aliasName, Indices: indexes}
				aliasesEntries = append(aliasesEntries, newEntry)
			}

		}

	} else {
		_, exists := allVirtualTableNames[indexPattern]
		if exists {
			newEntry := IndicesBody(indexPattern)
			currentAliases, err := virtualtable.GetAliasesAsArray(indexPattern, myid)
			if err != nil {
				log.Errorf("SendResolveIndexResponse: GetAliases returned err=%v", err)
				newEntry.Aliases = []string{}
			} else {
				newEntry.Aliases = currentAliases
			}
			indicesEntries = append(indicesEntries, newEntry)
		}

		aliasInfos, _ := virtualtable.GetAllAliasesAsMapArray(myid)
		for aliasName, indexes := range aliasInfos {
			if aliasName == indexPattern {
				newEntry := esutils.ResolveAliasEntry{Name: aliasName, Indices: indexes}
				aliasesEntries = append(aliasesEntries, newEntry)
			}
		}
	}
	return indicesEntries, aliasesEntries, nil
}

func isIndexExcluded(indexName string) bool {
	for _, value := range excludedInternalIndices {
		if strings.ReplaceAll(indexName, "*", "") == value {
			return true
		}
	}
	return false
}

func SendResolveIndexResponse(ctx *fasthttp.RequestCtx, myid uint64) {

	var resResp esutils.ResolveResponse

	indexPattern := utils.ExtractParamAsString(ctx.UserValue("indexPattern"))

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("Content-Type", "application/json")

	allVirtualTableNames, _ := virtualtable.GetVirtualTableNames(myid)

	if len(allVirtualTableNames) == 0 {
		resResp = esutils.ResolveResponse{}
		utils.WriteJsonResponse(ctx, resResp)
		return
	}

	indicesEntries, aliasesEntries, err := ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, myid)
	if err != nil {
		log.Errorf("SendResolveIndexResponse: Could not resolve index for indexPattern=%v err=%v", indexPattern, err)
		return
	}

	resResp = esutils.ResolveResponse{IndicesEntries: indicesEntries, AliasesEntries: aliasesEntries}
	utils.WriteJsonResponse(ctx, resResp)

}
