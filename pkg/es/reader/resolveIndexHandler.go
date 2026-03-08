// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

func IndicesBody(indexName string) esutils.ResolveIndexEntry {
	return esutils.ResolveIndexEntry{Name: indexName, Attributes: []string{"open"}}
}

func ExpandAndReturnIndexNames(indexPattern string, allVirtualTableNames map[string]bool, myid int64) ([]esutils.ResolveIndexEntry, []esutils.ResolveAliasEntry, error) {
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

func SendResolveIndexResponse(ctx *fasthttp.RequestCtx, myid int64) {

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
