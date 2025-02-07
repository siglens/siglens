// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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

package otlp

import (
	"testing"

	"github.com/siglens/siglens/pkg/utils"
	"github.com/siglens/siglens/pkg/virtualtable"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	collogpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func Test_Logs_BadContentType(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.Set("Content-Type", "application/foo")
	ProcessLogIngest(ctx, 0)

	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func Test_Logs_BadBody(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.Set("Content-Type", utils.ContentProtobuf)
	ctx.Request.SetBody([]byte("bad body"))
	ProcessLogIngest(ctx, 0)

	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func Test_Logs_FullSuccess(t *testing.T) {
	virtualtable.InitVTable(func() []int64 { return []int64{0} })
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.Set("Content-Type", utils.ContentProtobuf)
	protobuf := &collogpb.ExportLogsServiceRequest{
		ResourceLogs: []*logpb.ResourceLogs{{
			ScopeLogs: []*logpb.ScopeLogs{{
				LogRecords: []*logpb.LogRecord{{
					TimeUnixNano: 1234567890,
					SeverityText: "INFO",
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "hello world",
						},
					},
				}},
			}},
		}},
	}

	data, err := proto.Marshal(protobuf)
	assert.NoError(t, err)
	ctx.Request.SetBody(data)
	ProcessLogIngest(ctx, 0)

	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, utils.ContentProtobuf, string(ctx.Response.Header.Peek("Content-Type")))

	responseBytes := ctx.Response.Body()

	response := &collogpb.ExportLogsServiceResponse{}
	err = proto.Unmarshal(responseBytes, response)
	assert.NoError(t, err)

	// From https://opentelemetry.io/docs/specs/otlp/#full-success-1:
	// The server MUST leave the partial_success field unset in case of a successful response.
	assert.Nil(t, response.PartialSuccess)
}
