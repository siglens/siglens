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

package health

import (
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

func ProcessGetHealth(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse

	ctx.SetStatusCode(fasthttp.StatusOK)
	httpResp.StatusCode = fasthttp.StatusOK
	utils.WriteResponse(ctx, httpResp)

}

func ProcessSafeHealth(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse

	ctx.SetStatusCode(fasthttp.StatusOK)
	httpResp.StatusCode = fasthttp.StatusOK
	httpResp.Message = "Server started up in safe mode"
	utils.WriteResponse(ctx, httpResp)
}

func ProcessClusterHealthInfo(ctx *fasthttp.RequestCtx) {

	response := *utils.NewClusterHealthResponseInfo()
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, response)
}
