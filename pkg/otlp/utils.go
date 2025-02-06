package otlp

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

func getUncompressedData(ctx *fasthttp.RequestCtx) ([]byte, error) {
	data := ctx.PostBody()
	if requiresGzipDecompression(ctx) {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("cannot gzip decompress: err=%v", err)
		}

		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("cannot gzip decompress: err=%v", err)
		}
	}

	return data, nil
}

func requiresGzipDecompression(ctx *fasthttp.RequestCtx) bool {
	encoding := string(ctx.Request.Header.Peek("Content-Encoding"))
	if encoding == "gzip" {
		return true
	}

	if encoding != "" && encoding != "none" {
		log.Errorf("requiresGzipDecompression: invalid content encoding: %s. Request headers: %v", encoding, ctx.Request.Header.String())
	}

	return false
}

func setFailureResponse(ctx *fasthttp.RequestCtx, statusCode int, message string) {
	ctx.SetStatusCode(statusCode)

	failureStatus := status.Status{
		Code:    int32(statusCode),
		Message: message,
	}

	bytes, err := proto.Marshal(&failureStatus)
	if err != nil {
		log.Errorf("setFailureResponse: failed to marshal failure status. err: %v. Status: %+v", err, &failureStatus)
	}
	_, err = ctx.Write(bytes)
	if err != nil {
		log.Errorf("sendFailureResponse: failed to write failure status: %v", err)
	}
}
