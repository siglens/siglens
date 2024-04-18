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

package loki

type LokiMetricsResponse struct {
	Status string      `json:"status"`
	Data   MetricsData `json:"data"`
}

type MetricsData struct {
	ResultType   string        `json:"resultType"`
	MetricResult []MetricValue `json:"result,omitempty"`
	Stats        MetricStats   `json:"stats"`
}

type MetricValue struct {
	Stream map[string]interface{} `json:"metric"`
	Values []interface{}          `json:"value"`
}

type MetricStats struct {
	Summary  MetricsSummary  `json:"summary"`
	Querier  MetricsQuerier  `json:"querier"`
	Ingester MetricsIngester `json:"ingester"`
	Cache    Cache           `json:"cache"`
}

type MetricsIngester struct {
	TotalReached       int      `json:"totalReached"`
	TotalChunksMatched int      `json:"totalChunksMatched"`
	TotalBatches       int      `json:"totalBatches"`
	TotalLinesSent     int      `json:"totalLinesSent"`
	Store              SubStore `json:"store"`
}

type MetricsSummary struct {
	BytesProcessedPerSecond int     `json:"bytesProcessedPerSecond"`
	LinesProcessedPerSecond int     `json:"linesProcessedPerSecond"`
	TotalBytesProcessed     int     `json:"totalBytesProcessed"`
	TotalLinesProcessed     int     `json:"totalLinesProcessed"`
	ExecTime                float64 `json:"execTime"`
	QueueTime               float64 `json:"queueTime"`
	Subqueries              int     `json:"subqueries"`
	TotalEntriesReturned    int     `json:"totalEntriesReturned"`
	Splits                  int     `json:"splits"`
	Shards                  int     `json:"shards"`
}

type MetricsQuerier struct {
	Store SubStore `json:"store"`
}

type Cache struct {
	Chunk  CacheEnteries `json:"chunk"`
	Index  CacheEnteries `json:"index"`
	Result CacheEnteries `json:"result"`
}

type CacheEnteries struct {
	EntriesFound     int `json:"entriesFound"`
	EntriesRequested int `json:"entriesRequested"`
	EntriesStored    int `json:"entriesStored"`
	BytesReceived    int `json:"bytesReceived"`
	BytesSent        int `json:"bytesSent"`
	Requests         int `json:"requests"`
	DownloadTime     int `json:"downloadTime"`
}

type SubStore struct {
	TotalChunksRef        int     `json:"totalChunksRef"`
	TotalChunksDownloaded int     `json:"totalChunksDownloaded"`
	ChunksDownloadTime    float64 `json:"chunksDownloadTime"`
	Chunk                 Chunk   `json:"chunk"`
}

type Chunk struct {
	HeadChunkBytes    int `json:"headChunkBytes"`
	HeadChunkLines    int `json:"headChunkLines"`
	DecompressedBytes int `json:"decompressedBytes"`
	DecompressedLines int `json:"decompressedLines"`
	CompressedBytes   int `json:"compressedBytes"`
	TotalDuplicates   int `json:"totalDuplicates"`
}

type LokiQueryResponse struct {
	Status string `json:"status"`
	Data   Data   `json:"data"`
}
type Data struct {
	ResultType string        `json:"resultType"`
	Result     []StreamValue `json:"result"`
	Stats      Stats         `json:"stats"`
}

type StreamValue struct {
	Stream map[string]interface{} `json:"stream"`
	Values [][]string             `json:"values"`
}

type Stats struct {
	Ingester Ingester `json:"ingester"`
	Store    Store    `json:"store"`
	Summary  Summary  `json:"summary"`
}

type Ingester struct {
	CompressedBytes    int `json:"compressedBytes"`
	DecompressedBytes  int `json:"decompressedBytes"`
	DecompressedLines  int `json:"decompressedLines"`
	HeadChunkBytes     int `json:"headChunkBytes"`
	HeadChunkLines     int `json:"headChunkLines"`
	TotalBatches       int `json:"totalBatches"`
	TotalChunksMatched int `json:"totalChunksMatched"`
	TotalDuplicates    int `json:"totalDuplicates"`
	TotalLinesSent     int `json:"totalLinesSent"`
	TotalReached       int `json:"totalReached"`
}

type Store struct {
	CompressedBytes       int     `json:"compressedBytes"`
	DecompressedBytes     int     `json:"decompressedBytes"`
	DecompressedLines     int     `json:"decompressedLines"`
	ChunksDownloadTime    float64 `json:"chunksDownloadTime"`
	TotalChunksRef        int     `json:"totalChunksRef"`
	TotalChunksDownloaded int     `json:"totalChunksDownloaded"`
	TotalDuplicates       int     `json:"totalDuplicates"`
}

type Summary struct {
	BytesProcessedPerSecond int     `json:"bytesProcessedPerSecond"`
	ExecTime                float64 `json:"execTime"`
	LinesProcessedPerSecond int     `json:"linesProcessedPerSecond"`
	QueueTime               float64 `json:"queueTime"`
	TotalBytesProcessed     int     `json:"totalBytesProcessed"`
	TotalLinesProcessed     int     `json:"totalLinesProcessed"`
}
