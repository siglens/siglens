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
