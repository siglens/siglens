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
