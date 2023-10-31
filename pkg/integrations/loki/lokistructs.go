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
