// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"go.opentelemetry.io/otel/metric"
)

/*
TO DO -
1. Handle errors when creating new metrics
*/

var POST_REQUESTS_COUNT, _ = meter.Int64Counter(
	"ss.post.requests.count",
	metric.WithUnit("1"),
	metric.WithDescription("Counts post requests received"))

var QUERY_COUNT, _ = meter.Int64Counter(
	"ss.query.count",
	metric.WithUnit("1"),
	metric.WithDescription("query counts"))

var SEGFILE_ROTATE_COUNT, _ = meter.Int64Counter(
	"ss.segfile.rotate.count",
	metric.WithUnit("1"),
	metric.WithDescription("segment rotation count"))

var WIP_BUFFER_FLUSH_COUNT, _ = meter.Int64Counter(
	"ss.wip.buffer.flush.count",
	metric.WithUnit("1"),
	metric.WithDescription("wip flush count"))

var S3_UPLOADS, _ = meter.Int64Counter(
	"ss.s3uploads.received",
	metric.WithUnit("1"),
	metric.WithDescription("s3 uploads received"))

var S3_DOWNLOADS, _ = meter.Int64Counter(
	"ss.s3downloads.received",
	metric.WithUnit("1"),
	metric.WithDescription("s3 downloads received"))

var S3_DELETED, _ = meter.Int64Counter(
	"ss.s3deleted.received",
	metric.WithUnit("1"),
	metric.WithDescription("s3 deletes received"))
