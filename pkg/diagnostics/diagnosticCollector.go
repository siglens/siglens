package diagnostics

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/siglens/siglens/pkg/health"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/valyala/fasthttp"
)

func CollectDiagnosticsAPI(ctx *fasthttp.RequestCtx, orgid int64) {
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	respCtx := &fasthttp.RequestCtx{}
	respCtx.SetUserValue("originalContext", ctx)

	if hook := hooks.GlobalHooks.StatsHandlerHook; hook != nil {
		hook(respCtx, orgid)
	} else {
		health.ProcessClusterStatsHandler(respCtx, orgid)
	}

	var clusterStats map[string]interface{}
	if err := json.Unmarshal(respCtx.Response.Body(), &clusterStats); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to parse cluster stats: " + err.Error())
		return
	}

	if indexStats, ok := clusterStats["indexStats"].([]interface{}); ok {
		for _, indexData := range indexStats {
			if indexMap, ok := indexData.(map[string]interface{}); ok {
				for indexName, stats := range indexMap {
					indexBytes, err := json.MarshalIndent(stats, "", "  ")
					if err != nil {
						ctx.SetStatusCode(fasthttp.StatusInternalServerError)
						ctx.SetBodyString("Failed to marshal index stats: " + err.Error())
						return
					}

					indexFile, err := zipWriter.Create(fmt.Sprintf("%s.json", indexName))
					if err != nil {
						ctx.SetStatusCode(fasthttp.StatusInternalServerError)
						ctx.SetBodyString("Failed to create zip entry: " + err.Error())
						return
					}

					if _, err := indexFile.Write(indexBytes); err != nil {
						ctx.SetStatusCode(fasthttp.StatusInternalServerError)
						ctx.SetBodyString("Failed to write to zip entry: " + err.Error())
						return
					}
				}
			}
		}
	}

	if err := zipWriter.Close(); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to close zip file: " + err.Error())
		return
	}

	ctx.SetContentType("application/zip")
	ctx.Response.Header.Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=siglens-diagnostics-%s.zip",
			time.Now().Format("2006-01-02-15-04-05")))
	ctx.SetBody(buf.Bytes())
}
