// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
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
