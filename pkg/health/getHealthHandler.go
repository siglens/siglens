// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
