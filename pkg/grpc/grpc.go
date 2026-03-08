// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package grpc

// A single request for a segment key
type SegkeyRequest interface {
	GetSegmentKey() string
	GetTableName() string
	GetStartEpochMs() uint64
	GetEndEpochMs() uint64
}

type IngestFuncEnum uint32

const (
	INGEST_FUNC_UNKNOWN IngestFuncEnum = iota
	INGEST_FUNC_ES_BULK
	INGEST_FUNC_SPLUNK
	INGEST_FUNC_OTSDB_METRICS
	INGEST_FUNC_PROMETHEUS_METRICS
	INGEST_FUNC_OTLP_LOGS
	INGEST_FUNC_OTLP_TRACES
	INGEST_FUNC_OTLP_METRICS
	INGEST_FUNC_FAKE_DATA
	INGEST_FUNC_LOKI
)
