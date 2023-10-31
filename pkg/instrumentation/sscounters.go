/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
