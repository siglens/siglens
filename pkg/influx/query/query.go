package query

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func GetQueryHandler(ctx *fasthttp.RequestCtx, myid uint64) {

	q := string(ctx.QueryArgs().Peek("q"))
	if strings.ToLower(q) == "show databases" {
		databaseResponse := `{
			"results": [
				{
					"series": [
						{
							"name": "databases",
							"columns": ["name"],
							"values": [
								["_internal"],
								["benchmark_db"]
							]
						}
					]
				}
			]
		}`
		ctx.SetContentType("application/json")
		_, err := ctx.Write([]byte(databaseResponse))
		if err != nil {
			log.Errorf("GetQueryHandler: failed to write response, err=%v", err)
		}
	} else if strings.Contains(strings.ToLower(q), "create database") {
		// Return status code 200
		ctx.SetStatusCode(fasthttp.StatusOK)
	} else {
		ctx.Error("Unsupported query", fasthttp.StatusBadRequest)
	}
}
func PostQueryHandler(ctx *fasthttp.RequestCtx, myid uint64) {
	q := string(ctx.QueryArgs().Peek("q"))
	if strings.Contains(strings.ToLower(q), "drop database") {
		// Return status code 200
		ctx.SetStatusCode(fasthttp.StatusOK)
	} else {
		ctx.Error("Unsupported query", fasthttp.StatusBadRequest)
	}
}
