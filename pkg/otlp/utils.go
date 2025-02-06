package otlp

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

func getDataToUnmarshal(ctx *fasthttp.RequestCtx) ([]byte, error) {
	// We don't support JSON requests yet.
	if contentType := utils.GetContentType(ctx); contentType != utils.ContentProtobuf {
		return nil, fmt.Errorf("getDataToUnmarshal: got a non-protobuf request. Got Content-Type: %s", contentType)
	}

	// From https://opentelemetry.io/docs/specs/otlp/#otlphttp-response:
	// The server MUST use the same “Content-Type” in the response as it received in the request.
	utils.SetContentType(ctx, utils.ContentProtobuf)

	data, err := getUncompressedData(ctx)
	if err != nil {
		return nil, fmt.Errorf("getDataToUnmarshal: failed to uncompress data: %v", err)
	}

	return data, nil
}

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
