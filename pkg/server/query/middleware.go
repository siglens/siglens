// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package queryserver

import (
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/valyala/fasthttp"
)

func (hs *queryserverCfg) Recovery(next func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx) {
	fn := func(ctx *fasthttp.RequestCtx) {
		if hook := hooks.GlobalHooks.QueryMiddlewareRecoveryHook; hook != nil {
			err := hook(ctx)
			if err != nil {
				return
			}
		}

		// do next
		next(ctx)
	}
	return fn
}

func cors(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Headers", corsAllowHeaders)
		ctx.Response.Header.Set("Access-Control-Allow-Methods", corsAllowMethods)
		ctx.Response.Header.Set("Access-Control-Allow-Origin", corsAllowOrigin)
		next(ctx)
	}
}
