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

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/siglens/siglens/pkg/virtualtable"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	collogpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/genproto/googleapis/rpc/status"
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
	myid := int64(0)
	initTestConfig(t)
	err := virtualtable.InitVTable(func() []int64 { return []int64{myid} })
	assert.NoError(t, err)

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
	ProcessLogIngest(ctx, myid)

	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, utils.ContentProtobuf, string(ctx.Response.Header.Peek("Content-Type")))

	responseBytes := ctx.Response.Body()

	response := &collogpb.ExportLogsServiceResponse{}
	err = proto.Unmarshal(responseBytes, response)
	assert.NoError(t, err)

	// From https://opentelemetry.io/docs/specs/otlp/#full-success-1:
	// The server MUST leave the partial_success field unset in case of a successful response.
	assert.Nil(t, response.PartialSuccess)

	// Verify usage stats.
	assert.Equal(t, 1, int(usageStats.GetTotalLogLines(myid)))
}

// See https://opentelemetry.io/docs/specs/otlp/#partial-success-1
func Test_Logs_PartialSuccess(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	numTotal := 10
	numFailed := 5
	setLogIngestionResponse(ctx, numTotal, numFailed)

	response := &collogpb.ExportLogsServiceResponse{}
	err := proto.Unmarshal(ctx.Response.Body(), response)
	assert.NoError(t, err)

	assert.NotNil(t, response.PartialSuccess)
	assert.Equal(t, int64(numFailed), response.PartialSuccess.RejectedLogRecords)

	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

// See https://opentelemetry.io/docs/specs/otlp/#failures-1
func Test_Logs_Failure(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	numTotal := 10
	numFailed := 10
	setLogIngestionResponse(ctx, numTotal, numFailed)

	response := &status.Status{}
	err := proto.Unmarshal(ctx.Response.Body(), response)
	assert.NoError(t, err)

	statusCode := ctx.Response.StatusCode()
	assert.True(t, statusCode >= 400 && statusCode < 600, "invalid status code: %d", statusCode)
}

func initTestConfig(t *testing.T) {
	runningConfig := config.GetTestConfig(t.TempDir())
	config.SetConfig(runningConfig)

	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
}

func Test_extractLogRecord_KvlistValue(t *testing.T) {
	protobuf := &collogpb.ExportLogsServiceRequest{
		ResourceLogs: []*logpb.ResourceLogs{{
			ScopeLogs: []*logpb.ScopeLogs{{
				LogRecords: []*logpb.LogRecord{{
					TimeUnixNano: 1234567890,
					SeverityText: "INFO",
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_KvlistValue{
							KvlistValue: &commonpb.KeyValueList{
								Values: []*commonpb.KeyValue{
									{
										Key: "key1",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "value1",
											},
										},
									},
									{
										Key: "key2",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_IntValue{
												IntValue: 2,
											},
										},
									},
									{
										Key: "key3",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_DoubleValue{
												DoubleValue: 3.0,
											},
										},
									},
									{
										Key: "key4",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_BoolValue{
												BoolValue: true,
											},
										},
									},
									{
										Key: "key5",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_ArrayValue{
												ArrayValue: &commonpb.ArrayValue{
													Values: []*commonpb.AnyValue{
														{
															Value: &commonpb.AnyValue_StringValue{
																StringValue: "value1",
															},
														},
														{
															Value: &commonpb.AnyValue_IntValue{
																IntValue: 5,
															},
														},
													},
												},
											},
										},
									},
									{
										Key: "key6",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_KvlistValue{
												KvlistValue: &commonpb.KeyValueList{
													Values: []*commonpb.KeyValue{
														{
															Key: "nest1Key1",
															Value: &commonpb.AnyValue{
																Value: &commonpb.AnyValue_StringValue{
																	StringValue: "nest1Value1",
																},
															},
														},
														{
															Key: "nest1Key2",
															Value: &commonpb.AnyValue{
																Value: &commonpb.AnyValue_IntValue{
																	IntValue: 6,
																},
															},
														},
														{
															Key: "nest1Key3",
															Value: &commonpb.AnyValue{
																Value: &commonpb.AnyValue_KvlistValue{
																	KvlistValue: &commonpb.KeyValueList{
																		Values: []*commonpb.KeyValue{
																			{
																				Key: "nest2Key1",
																				Value: &commonpb.AnyValue{
																					Value: &commonpb.AnyValue_StringValue{
																						StringValue: "nest2Value1",
																					},
																				},
																			},
																			{
																				Key: "nest2Key2",
																				Value: &commonpb.AnyValue{
																					Value: &commonpb.AnyValue_IntValue{
																						IntValue: 7,
																					},
																				},
																			},
																			{
																				Key: "nest2Key3",
																				Value: &commonpb.AnyValue{
																					Value: &commonpb.AnyValue_KvlistValue{
																						KvlistValue: &commonpb.KeyValueList{
																							Values: []*commonpb.KeyValue{
																								{
																									Key: "nest3Key1",
																									Value: &commonpb.AnyValue{
																										Value: &commonpb.AnyValue_ArrayValue{
																											ArrayValue: &commonpb.ArrayValue{
																												Values: []*commonpb.AnyValue{
																													{
																														Value: &commonpb.AnyValue_StringValue{
																															StringValue: "nest3Value1",
																														},
																													},
																													{
																														Value: &commonpb.AnyValue_IntValue{
																															IntValue: 8,
																														},
																													},
																												},
																											},
																										},
																									},
																								},
																							},
																						},
																					},
																				},
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}},
			}},
		}},
	}

	recordInfo, err := extractLogRecord(protobuf.ResourceLogs[0].ScopeLogs[0].LogRecords[0], nil, nil)
	assert.NoError(t, err)

	assert.Equal(t, uint64(1234567890), recordInfo.TimeUnixNano)
	assert.Equal(t, "INFO", recordInfo.SeverityText)

	expectedBody := map[string]interface{}{
		"key1": "value1",
		"key2": int64(2),
		"key3": 3.0,
		"key4": true,
		"key5": []interface{}{"value1", int64(5)},
		"key6": map[string]interface{}{
			"nest1Key1": "nest1Value1",
			"nest1Key2": int64(6),
			"nest1Key3": map[string]interface{}{
				"nest2Key1": "nest2Value1",
				"nest2Key2": int64(7),
				"nest2Key3": map[string]interface{}{
					"nest3Key1": []interface{}{"nest3Value1", int64(8)},
				},
			},
		},
	}

	assert.Equal(t, expectedBody, recordInfo.Body)
}
