package otlp

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
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

func extractKeyValue(keyvalue *commonpb.KeyValue) (string, interface{}, error) {
	value, err := extractAnyValue(keyvalue.Value)
	if err != nil {
		log.Errorf("extractKeyValue: failed to extract value for key %s: %v", keyvalue.Key, err)
		return "", nil, err
	}

	return keyvalue.Key, value, nil
}

func extractAnyValue(anyValue *commonpb.AnyValue) (interface{}, error) {
	switch anyValue.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return anyValue.GetStringValue(), nil
	case *commonpb.AnyValue_IntValue:
		return anyValue.GetIntValue(), nil
	case *commonpb.AnyValue_DoubleValue:
		return anyValue.GetDoubleValue(), nil
	case *commonpb.AnyValue_BoolValue:
		return anyValue.GetBoolValue(), nil
	case *commonpb.AnyValue_ArrayValue:
		arrayValue := anyValue.GetArrayValue().Values
		value := make([]interface{}, len(arrayValue))
		for i := range arrayValue {
			var err error
			value[i], err = extractAnyValue(arrayValue[i])
			if err != nil {
				return nil, err
			}
		}

		return value, nil
	default:
		return nil, fmt.Errorf("extractAnyValue: unsupported value type: %T", anyValue.Value)
	}
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
