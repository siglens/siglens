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
