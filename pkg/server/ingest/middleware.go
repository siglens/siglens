// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package ingestserver

import (
	"github.com/siglens/siglens/pkg/hooks"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func (hs *ingestionServerCfg) Recovery(next func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx) {
	fn := func(ctx *fasthttp.RequestCtx) {
		if hook := hooks.GlobalHooks.IngestMiddlewareRecoveryHook; hook != nil {
			err := hook(ctx)
			if err != nil {
				log.Errorf("ingestionServerCfg.Recovery: got error from hook: %v", err)
				return
			}
		}

		// do next
		next(ctx)
	}
	return fn
}
