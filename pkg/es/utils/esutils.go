// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package esutils

import (
	"github.com/google/uuid"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

type ResolveIndexEntry struct {
	Name       string   `json:"name"`
	Attributes []string `json:"attributes"`
	Aliases    []string `json:"aliases"`
}

type ResolveAliasEntry struct {
	Name    string   `json:"name"`
	Indices []string `json:"indices"`
}

type ResolveResponse struct {
	IndicesEntries []ResolveIndexEntry `json:"indices"`
	AliasesEntries []ResolveAliasEntry `json:"aliases"`
}

func ProcessGreetHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.Set("X-elastic-product", "Elasticsearch")
	uuidVal := uuid.New().String()
	greetresp := utils.NewGreetResponse(config.GetHostname(), uuidVal, *config.GetESVersion())
	utils.WriteJsonResponse(ctx, greetresp)
}
