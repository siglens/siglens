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

package utils

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"

	"github.com/cespare/xxhash"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const (
	ContentJson = "application/json; charset=utf-8"
)

type HttpServerResponse struct {
	Message    string `json:"message"`
	StatusCode int    `json:"status"`
}

type HttpServerSettingsResponse struct {
	Persistent map[string]string `json:"persistent"`
	Transient  map[string]string `json:"transient"`
}

type Hits struct {
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	Id      string                 `json:"_id"`
	Version int                    `json:"_version"`
	Score   int                    `json:"_score"`
	Source  map[string]interface{} `json:"_source"`
}

type HitsCount struct {
	Value    uint64 `json:"value"`
	Relation string `json:"relation"`
}
type HttpServerESResponse struct {
	Hits      []Hits      `json:"hits"`
	Max_score int         `json:"max_score"`
	Total     interface{} `json:"total"`
}

type StatResponse struct {
	Value interface{} `json:"value"`
}

type BucketWrapper struct {
	Bucket []map[string]interface{} `json:"buckets"`
}

type HttpServerESResponseOuter struct {
	Hits       HttpServerESResponse     `json:"hits"`
	Aggs       map[string]BucketWrapper `json:"aggregations"`
	Took       int64                    `json:"took"`
	Timed_out  bool                     `json:"timed_out"`
	StatusCode int                      `json:"status"`
	Shards     map[string]interface{}   `json:"_shards"`
}

type MultiSearchESResponse struct {
	Results []HttpServerESResponseOuter `json:"responses"`
}

type HttpServerESResponseScroll struct {
	Hits       HttpServerESResponse     `json:"hits"`
	Aggs       map[string]BucketWrapper `json:"aggregations"`
	Took       int64                    `json:"took"`
	Timed_out  bool                     `json:"timed_out"`
	StatusCode int                      `json:"status"`
	Scroll_id  string                   `json:"_scroll_id"`
}

type SingleESResponse struct {
	Index          string                 `json:"_index"`
	Type           string                 `json:"_type"`
	Id             string                 `json:"_id"`
	Version        int                    `json:"_version"`
	SequenceNumber int                    `json:"_seq_no"`
	Source         map[string]interface{} `json:"_source"`
	Found          bool                   `json:"found"`
	PrimaryTerm    int                    `json:"_primary_term"`
}

type DocIndexedResponse struct {
	Index          string                        `json:"_index"`
	Type           string                        `json:"_type"`
	Id             string                        `json:"_id"`
	Version        int                           `json:"_version"`
	SequenceNumber int                           `json:"_seq_no"`
	Result         string                        `json:"result"`
	PrimaryTerm    int                           `json:"_primary_term"`
	Shards         map[string]interface{}        `json:"_shards"`
	Get            DocIndexedResponseSubFieldGet `json:"get"`
}

type DocIndexedResponseSubFieldGet struct {
	SequenceNumber int                    `json:"_seq_no"`
	PrimaryTerm    int                    `json:"_primary_term"`
	Found          bool                   `json:"found"`
	Source         map[string]interface{} `json:"_source"`
}

type IndexNameInfoResponse struct {
	Aliases  map[string]bool        `json:"aliases"`
	Mappings map[string]interface{} `json:"mappings"`
	Settings SettingsInfo           `json:"settings"`
}
type NodesStatsResponseInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type NodesStatsMemResponse struct {
	HeapCommitted           string `json:"heap_committed"`
	HeapCommittedInBytes    uint64 `json:"heap_committed_in_bytes"`
	HeapMax                 string `json:"heap_max"`
	HeapMaxInBytes          uint64 `json:"heap_max_in_bytes"`
	HeapUsed                string `json:"heap_used"`
	HeapUsedInBytes         uint64 `json:"heap_used_in_bytes"`
	HeapUsedPercent         int    `json:"heap_used_percent"`
	NonHeapCommitted        string `json:"non_heap_committed"`
	NonHeapCommittedInBytes int    `json:"non_heap_committed_in_bytes"`
	NonHeapUsed             string `json:"non_heap_used"`
	NonHeapUsedInBytes      int    `json:"non_heap_used_bytes"`
}
type NodesResponseInfo struct {
	Timestamp        int64                  `json:"timestamp"`
	Name             string                 `json:"name"`
	TransportAddress string                 `json:"transport_address"`
	HostName         string                 `json:"host"`
	IP               string                 `json:"ip"`
	OSResponse       map[string]interface{} `json:"os"`
	JVMResponse      map[string]interface{} `json:"jvm"`
	FSResponse       map[string]interface{} `json:"fs"`
	ProcessResponse  map[string]interface{} `json:"process"`
}

type AllResponseInfo struct {
	Primaries map[string]interface{} `json:"primaries"`
	Total     map[string]interface{} `json:"total"`
}
type LoadAverageResponseInfo struct {
	LoadResponse float64 `json:"1m"`
}
type SettingsInfo struct {
	Index SettingsIndexInfo `json:"index"`
}

type SettingsIndexInfo struct {
	NumberOfShards   int    `json:"number_of_shards"`
	NumberOfReplicas int    `json:"number_of_replicas"`
	ProvidedName     string `json:"provided_name"`
}
type BulkErrorResponse struct {
	ErrorResponse BulkErrorResponseInfo `json:"error"`
}
type BulkErrorResponseInfo struct {
	Reason string `json:"reason"`
	Type   string `json:"type"`
}
type MgetESResponse struct {
	Docs []SingleESResponse `json:"docs"`
}
type AllNodesInfoResponse struct {
	Nodes map[string]*NodeInfo `json:"nodes"`
}

type MemResponseInfo struct {
	Total        string `json:"total"`
	TotalInBytes uint64 `json:"total_in_bytes"`
	Free         string `json:"free"`
	FreeInBytes  uint64 `json:"free_in_bytes"`
	Used         string `json:"used"`
	UsedInBytes  uint64 `json:"used_in_bytes"`
	FreePercent  uint64 `json:"free_percent"`
	UsedPercent  uint64 `json:"used_percent"`
}
type VirtualMemResponse struct {
	TotalVirtual        string `json:"total_virtual"`
	TotalVirtualInBytes uint64 `json:"total_virtual_in_bytes"`
}
type SwapResponseInfo struct {
	Total        string `json:"total"`
	TotalInBytes int64  `json:"total_in_bytes"`
	Free         string `json:"free"`
	FreeInBytes  int64  `json:"free_in_bytes"`
	Used         string `json:"used"`
	UsedInBytes  int64  `json:"used_in_bytes"`
}

type NodesTransportResponseInfo struct {
	Name             string `json:"name"`
	TransportAddress string `json:"transport_address"`
	HostName         string `json:"host"`
	IP               string `json:"ip"`
	Version          string `json:"version"`
	BuildFlavor      string `json:"build_flavor"`
	BuildType        string `json:"build_type"`
	BuildHash        string `json:"build_hash"`
}

type ClusterHealthResponseInfo struct {
	ClusterName          string  `json:"cluster_name"`
	Status               string  `json:"status"`
	TimedOut             bool    `json:"timed_out"`
	NumberOfNodes        int     `json:"number_of_nodes"`
	NumberOfDataNodes    int     `json:"number_of_data_nodes"`
	ActivePrimaryShards  int     `json:"active_primary_shards"`
	ActiveShards         int     `json:"active_shards"`
	RelocatingShards     int     `json:"relocating_shards"`
	InitiliazeShards     int     `json:"initializing_shards"`
	UnassignedShards     int     `json:"unassigned_shards"`
	DelayedShards        int     `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks int     `json:"number_of_pending_tasks"`
	NumberInFlightFetch  int     `json:"number_of_in_flight_fetch"`
	TaskMaxWaiting       int     `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercent  float64 `json:"active_shards_percent_as_number"`
}

type DocsResponse struct {
	Count   int `json:"count"`
	Deleted int `json:"deleted"`
}
type StoreResponse struct {
	SizeInBytes     uint64 `json:"size_in_bytes"`
	ReservedInBytes int    `json:"reservec_in_bytes"`
}
type ClusterStateResponseInfo struct {
	ClusterName  string                 `json:"cluster_name"`
	ClusterUUID  string                 `json:"cluster_uuid"`
	MasterNode   string                 `json:"master_node"`
	Blocks       map[string]interface{} `json:"blocks"`
	RoutingTable map[string]interface{} `json:"routing_table"`
}

type ClusterStateBlocksInfo struct {
	ClusterName string                 `json:"cluster_name"`
	ClusterUUID string                 `json:"cluster_uuid"`
	Blocks      map[string]interface{} `json:"blocks"`
}

type NodesAllResponseInfo struct {
	Name             string              `json:"name"`
	TransportAddress string              `json:"transport_address"`
	HostName         string              `json:"host"`
	IP               string              `json:"ip"`
	Version          string              `json:"version"`
	BuildFlavor      string              `json:"build_flavor"`
	BuildType        string              `json:"build_type"`
	BuildHash        string              `json:"build_hash"`
	OSResponse       NodesOSResponseInfo `json:"os"`
}
type NodesOSResponseInfo struct {
	RefreshInterval     string `json:"refresh_interval"`
	RefreshIntervalInMS int    `json:"refresh_interval_in_millis"`
	Name                string `json:"name"`
	PrettyName          string `json:"pretty_name"`
	Arch                string `json:"arch"`
	Version             string `json:"version"`
	AvailableProcessors int    `json:"available_processors"`
	AllocatedProcessors int    `json:"allocated_processors"`
}

type ShardsItemsResponse struct {
	State          string                 `json:"state"`
	Primary        bool                   `json:"primary"`
	Node           string                 `json:"node"`
	RelocatingNode *string                `json:"relocating_node"`
	Shard          int                    `json:"shard"`
	Index          string                 `json:"index"`
	AllocationId   map[string]interface{} `json:"allocation_id"`
}
type AllIndicesInfoResponse []IndexInfo

type MultiNodesInfoResponse []*MemberInfo

type GreetResponse struct {
	Name        string       `json:"name"`
	ClusterName string       `json:"cluster_name"`
	ClusterUUID string       `json:"cluster_uuid"`
	Version     GreetVersion `json:"version"`
}

type GreetVersion struct {
	Number                           string `json:"number"`
	BuildFlavour                     string `json:"build_flavor"`
	BuildType                        string `json:"build_type"`
	BuildDate                        string `json:"build_date"`
	BuildHash                        string `json:"build_hash"`
	BuildSnapshot                    bool   `json:"build_snapshot"`
	LuceneVersion                    string `json:"lucene_version"`
	MinimumWireCompatibilityVersion  string `json:"minimum_wire_compatibility_version"`
	MinimumIndexCompatibilityVersion string `json:"minimum_index_compatibility_version"`
}
type TotalFsResponse struct {
	Total            string `json:"total"`
	TotalInBytes     uint64 `json:"total_in_bytes"`
	Free             string `json:"free"`
	FreeInBytes      uint64 `json:"free_in_bytes"`
	Available        string `json:"available"`
	AvailableInBytes uint64 `json:"available_in_bytes"`
}
type ProcessCpuResponse struct {
	Percent       int    `json:"percent"`
	Total         string `json:"total"`
	TotalInMillis uint64 `json:"total_in_millis"`
}
type GetMasterResponse struct {
	Id   string `json:"id"`
	Host string `json:"host"`
	IP   string `json:"ip"`
	Node string `json:"node"`
}
type NodesStatsIdInfo struct {
	Timestamp        int64  `json:"timestamp"`
	Name             string `json:"name"`
	TransportAddress string `json:"transport_address"`
	Host             string `json:"host"`
	IP               string `json:"ip"`
}

type NodesStatsIngestInfo struct {
	Name             string                 `json:"name"`
	TransportAddress string                 `json:"transport_address"`
	Host             string                 `json:"host"`
	IP               string                 `json:"ip"`
	Version          string                 `json:"version"`
	BuildFlavour     string                 `json:"build_flavor"`
	BuildType        string                 `json:"build_type"`
	BuildHash        string                 `json:"build_hash"`
	Roles            []string               `json:"roles"`
	Ingest           map[string]interface{} `json:"ingest"`
}

type DeleteIndexErrorResponseInfo struct {
	Type         string `json:"type"`
	Reason       string `json:"reason"`
	ResourceType string `json:"resource.type"`
	ResourceId   string `json:"resource.id"`
	IndexUUID    string `json:"index.uuid"`
	Index        string `json:"index"`
}

type ResultPerIndex map[string]map[string]interface{} // maps index name to index stats

type ClusterStatsResponseInfo struct {
	IngestionStats map[string]interface{}            `json:"ingestionStats"`
	QueryStats     map[string]interface{}            `json:"queryStats"`
	MetricsStats   map[string]interface{}            `json:"metricsStats"`
	IndexStats     []ResultPerIndex                  `json:"indexStats"`
	ChartStats     map[string]map[string]interface{} `json:"chartStats"`
}

type MetricsStatsResponseInfo struct {
	AggStats map[string]map[string]interface{} `json:"aggStats"`
}

type AllSavedQueries map[string]map[string]interface{}

type AutoCompleteDataInfo struct {
	ColumnNames      []string `json:"colNames"`
	MeasureFunctions []string `json:"measureFunctions"`
}

func NewSingleESResponse() *SingleESResponse {
	return &SingleESResponse{
		Index:          "",
		Type:           "",
		Id:             "",
		Version:        1,
		SequenceNumber: 0,
		Source:         make(map[string]interface{}),
		Found:          false,
		PrimaryTerm:    1,
	}
}

func WriteResponse(ctx *fasthttp.RequestCtx, httpResp HttpServerResponse) {
	ctx.SetContentType(ContentJson)
	jval, _ := json.Marshal(httpResp)
	_, err := ctx.Write(jval)
	if err != nil {
		return
	}
}

func WriteJsonResponse(ctx *fasthttp.RequestCtx, httpResp interface{}) {
	ctx.SetContentType(ContentJson)
	jval, _ := json.Marshal(httpResp)
	_, err := ctx.Write(jval)
	if err != nil {
		return
	}
}

func SetBadMsg(ctx *fasthttp.RequestCtx, msg string) {
	if len(msg) == 0 {
		msg = "Bad Request"
	}
	var httpResp HttpServerResponse
	ctx.SetStatusCode(fasthttp.StatusBadRequest)
	httpResp.Message = msg
	httpResp.StatusCode = fasthttp.StatusBadRequest
	WriteResponse(ctx, httpResp)
}

func ExtractParamAsString(_interface interface{}) string {
	switch intfc := _interface.(type) {
	case string:
		return intfc
	default:
		return ""
	}
}

func NewSettingsIndexInfo(providedName string) *SettingsIndexInfo {
	return &SettingsIndexInfo{
		NumberOfShards:   1,
		NumberOfReplicas: 1,
		ProvidedName:     providedName,
	}
}

func NewBulkErrorResponseInfo(reason string, typ string) *BulkErrorResponseInfo {
	return &BulkErrorResponseInfo{
		Reason: reason,
		Type:   typ,
	}
}

func NewNodesStatsResponseInfo(total int, successful int, failed int) *NodesStatsResponseInfo {
	return &NodesStatsResponseInfo{
		Total:      total,
		Successful: successful,
		Failed:     failed,
	}
}

func NewNodesResponseInfo(timestamp int64, host string, addressPort string, ipAddress string, osResponse map[string]interface{}, jvmResponse map[string]interface{}, fsResponse map[string]interface{}, processResponse map[string]interface{}) *NodesResponseInfo {
	return &NodesResponseInfo{
		Timestamp:        timestamp,
		Name:             host,
		HostName:         ipAddress,
		TransportAddress: addressPort,
		IP:               ipAddress,
		OSResponse:       osResponse,
		JVMResponse:      jvmResponse,
		FSResponse:       fsResponse,
		ProcessResponse:  processResponse,
	}
}

func NewLoadAverageResponseInfo(loadAverage float64) *LoadAverageResponseInfo {
	return &LoadAverageResponseInfo{
		LoadResponse: loadAverage,
	}
}

func NewAllResponseInfo() *AllResponseInfo {
	return &AllResponseInfo{
		Primaries: make(map[string]interface{}),
		Total:     make(map[string]interface{}),
	}
}

func NewMemResponseInfo(total string, totalInBytes uint64, free string, freeInBytes uint64, used string, usedInBytes uint64, freePercent float64, usedPercent float64) *MemResponseInfo {
	return &MemResponseInfo{
		Total:        total,
		TotalInBytes: totalInBytes,
		Free:         free,
		FreeInBytes:  freeInBytes,
		Used:         used,
		UsedInBytes:  usedInBytes,
		FreePercent:  uint64(freePercent),
		UsedPercent:  uint64(usedPercent),
	}
}

func NewSwapResponseInfo() *SwapResponseInfo {
	return &SwapResponseInfo{
		Total:        "3gb",
		TotalInBytes: 3221225472,
		Free:         "816.2mb",
		FreeInBytes:  855900160,
		Used:         "2.2gb",
		UsedInBytes:  2365325312,
	}
}

func NewNodesTransportResponseInfo(host string, addressPort string, version string) *NodesTransportResponseInfo {
	return &NodesTransportResponseInfo{
		Name:             host,
		TransportAddress: addressPort,
		HostName:         addressPort,
		IP:               addressPort,
		Version:          version,
		BuildFlavor:      "oss",
		BuildType:        "tar",
		BuildHash:        "c4138e51121ef06a6404866cddc601906fe5c868",
	}
}

func NewClusterStateResponseInfo(uuidVal string, indicesResponse map[string]interface{}) *ClusterStateResponseInfo {
	return &ClusterStateResponseInfo{
		ClusterName:  "siglens",
		ClusterUUID:  uuidVal,
		MasterNode:   "QXiD6Xa-RVqdZSLL8I_ZpQ",
		Blocks:       make(map[string]interface{}),
		RoutingTable: indicesResponse,
	}
}

func NewClusterStateBlocksInfo(uuidVal string) *ClusterStateBlocksInfo {
	return &ClusterStateBlocksInfo{
		ClusterName: "siglens",
		ClusterUUID: uuidVal,
		Blocks:      make(map[string]interface{}),
	}
}

func NewNodesAllResponseInfo(host string, addressPort string, version string, osResponse NodesOSResponseInfo) *NodesAllResponseInfo {
	return &NodesAllResponseInfo{
		Name:             host,
		TransportAddress: addressPort,
		HostName:         addressPort,
		IP:               addressPort,
		Version:          version,
		BuildFlavor:      "oss",
		BuildType:        "tar",
		BuildHash:        "c4138e51121ef06a6404866cddc601906fe5c868",
		OSResponse:       osResponse,
	}
}

func NewNodesOSResponseInfo() *NodesOSResponseInfo {
	return &NodesOSResponseInfo{
		RefreshInterval:     "1s",
		RefreshIntervalInMS: 1000,
		Name:                "Mac OS X",
		PrettyName:          "Mac OS X",
		Arch:                "aarch64",
		Version:             "11.3",
		AvailableProcessors: 8,
		AllocatedProcessors: 8,
	}
}

func NewClusterHealthResponseInfo() *ClusterHealthResponseInfo {
	return &ClusterHealthResponseInfo{
		ClusterName:          "siglens",
		Status:               "green",
		TimedOut:             false,
		NumberOfNodes:        1,
		NumberOfDataNodes:    1,
		ActivePrimaryShards:  0,
		ActiveShards:         0,
		RelocatingShards:     0,
		InitiliazeShards:     0,
		UnassignedShards:     0,
		DelayedShards:        0,
		NumberOfPendingTasks: 0,
		NumberInFlightFetch:  0,
		TaskMaxWaiting:       0,
		ActiveShardsPercent:  100.0,
	}
}

func NewNodesStatsMemResponse(heapcommitted string, heapCommittedBytes uint64, heapMax string, heapMaxBytes uint64, heapUsed string, heapUsedBytes uint64) *NodesStatsMemResponse {
	return &NodesStatsMemResponse{
		HeapCommitted:           heapcommitted,
		HeapCommittedInBytes:    heapCommittedBytes,
		HeapMax:                 heapMax,
		HeapMaxInBytes:          heapMaxBytes,
		HeapUsed:                heapUsed,
		HeapUsedInBytes:         heapUsedBytes,
		HeapUsedPercent:         2,
		NonHeapCommitted:        "80.6mb",
		NonHeapCommittedInBytes: 84606976,
		NonHeapUsed:             "77.9mb",
		NonHeapUsedInBytes:      81756968,
	}
}
func NewDocsResponse(evenCount int) *DocsResponse {
	return &DocsResponse{
		Count:   evenCount,
		Deleted: 0,
	}
}
func NewStoreResponse(bytesReceivedCount uint64) *StoreResponse {
	return &StoreResponse{
		SizeInBytes:     bytesReceivedCount,
		ReservedInBytes: 0,
	}
}
func NewShardsItemsResponse(indexName string, allocationResponse map[string]interface{}) *ShardsItemsResponse {
	return &ShardsItemsResponse{
		State:          "STARTED",
		Primary:        true,
		Node:           "Ic2KfXpfQhaWhgeXAAcMYw",
		RelocatingNode: nil,
		Shard:          0,
		Index:          indexName,
		AllocationId:   allocationResponse,
	}
}
func NewGreetVersion(esVersion string) GreetVersion {
	return GreetVersion{
		Number:                           esVersion,
		BuildFlavour:                     "oss",
		BuildType:                        "tar",
		BuildDate:                        "2021-10-07T21:56:19.031608185Z",
		BuildHash:                        "83c34f456ae29d60e94d886e455e6a3409bba9ed",
		BuildSnapshot:                    false,
		LuceneVersion:                    "8.9.0",
		MinimumWireCompatibilityVersion:  "6.8.0",
		MinimumIndexCompatibilityVersion: "6.0.0-beta1",
	}
}

func NewGreetResponse(host string, uuidVal string, esVersion string) GreetResponse {
	return GreetResponse{
		Name:        host,
		ClusterName: "siglens",
		ClusterUUID: uuidVal,
		Version:     NewGreetVersion(esVersion),
	}
}
func NewTotalFsResponse(total string, totalInBytes uint64, free string, freeInBytes uint64, available string, availableInBytes uint64) *TotalFsResponse {
	return &TotalFsResponse{
		Total:            total,
		TotalInBytes:     totalInBytes,
		Free:             free,
		FreeInBytes:      freeInBytes,
		Available:        available,
		AvailableInBytes: totalInBytes - availableInBytes,
	}
}
func NewVirtualMemResponse(totalVirtual string, totalVirtualBytes uint64) *VirtualMemResponse {
	return &VirtualMemResponse{
		TotalVirtual:        totalVirtual,
		TotalVirtualInBytes: totalVirtualBytes,
	}
}
func NewProcessCpuResponse(percent int) *ProcessCpuResponse {
	return &ProcessCpuResponse{
		Percent:       percent,
		Total:         "10.2s",
		TotalInMillis: 10256,
	}
}
func NewGetMasterResponse(id string, host string, node string) *GetMasterResponse {
	return &GetMasterResponse{
		Id:   id,
		Host: host,
		IP:   host,
		Node: node,
	}
}
func NewNodesStatsIdInfo(timestamp int64, hostname string, addressPort string, ipAddress string) *NodesStatsIdInfo {
	return &NodesStatsIdInfo{
		Timestamp:        timestamp,
		Name:             hostname,
		TransportAddress: addressPort,
		Host:             ipAddress,
		IP:               ipAddress,
	}
}

func NewNodesStatsIngestInfo(hostname string, addressPort string,
	ipAddress string, version string, ingestFeats map[string]interface{}) *NodesStatsIngestInfo {
	return &NodesStatsIngestInfo{
		Name:             hostname,
		TransportAddress: addressPort,
		Host:             ipAddress,
		IP:               ipAddress,
		Version:          version,
		BuildFlavour:     "oss",
		BuildType:        "tar",
		BuildHash:        "83c34f456ae29d60e94d886e455e6a3409bba9ed",
		Roles:            []string{"data", "ingest", "master", "remote_cluster_client"},
		Ingest:           ingestFeats,
	}
}

func NewDeleteIndexErrorResponseInfo(indexName string) *DeleteIndexErrorResponseInfo {
	return &DeleteIndexErrorResponseInfo{
		Type:         "index_not_found_exception",
		Reason:       "no such index" + "[" + indexName + "]",
		ResourceType: "index_or_alias",
		ResourceId:   indexName,
		IndexUUID:    "_na_",
		Index:        indexName,
	}
}

func NewAutoCompleteDataInfo(columnNames []string, measureFunctions []string) *AutoCompleteDataInfo {
	return &AutoCompleteDataInfo{
		ColumnNames:      columnNames,
		MeasureFunctions: measureFunctions,
	}
}

func (eo *HttpServerESResponse) GetHits() uint64 {
	switch t := eo.Total.(type) {
	case uint64:
		return t
	case HitsCount:
		return t.Value
	case map[string]interface{}:
		retVal, ok := t["value"]
		if !ok {
			log.Infof("Tried to get hits for a map with no 'value' key! Map: %v", retVal)
			return 0
		}
		switch hit := retVal.(type) {
		case float64:
			return uint64(hit)
		case uint64:
			return hit
		case int64:
			return uint64(hit)
		default:
			log.Infof("Map value is not a supported type!: %v %T", hit, hit)
			return 0
		}
	default:
		log.Infof("Tried to get hits for unsupported type %T", t)
		return 0
	}
}

func ExtractBearerToken(ctx *fasthttp.RequestCtx) (string, error) {
	authHeader := ctx.Request.Header.Peek("Authorization")
	authToken := strings.Split(string(authHeader), "Bearer ")
	if len(authToken) != 2 {
		return "", errors.New("malformed bearer token")
	}
	jwtToken := authToken[1]
	return jwtToken, nil
}

// Reads the basic authorization from the request to extract the username and
// password. Then hashes the username and password and compares with the
// expected hash values.
// Returns true if the hashes match. Returns false if the hashes do not match
// or if basic authentication credentials are not provided in the request.
// The provided usernameHash and passwordHash fields should be hashed with
// the xxhash.Sum64 algorithm.
// Basic authentication is defined at https://www.rfc-editor.org/rfc/rfc2617#section-2
func VerifyBasicAuth(ctx *fasthttp.RequestCtx, usernameHash uint64, passwordHash uint64) bool {
	auth := string(ctx.Request.Header.Peek("Authorization"))
	if len(auth) == 0 {
		return false
	}

	// The auth string should be something like:
	// Basic dXNlcm5hbWU6cGFzc3dvcmQK
	const prefix = "Basic "
	if !strings.HasPrefix(auth, prefix) {
		return false
	}

	base64Encoding := auth[len(prefix):]
	decoded, err := base64.StdEncoding.DecodeString(base64Encoding)
	if err != nil {
		log.Errorf("VerifyBasicAuth: failed to decode %s: %v", base64Encoding, err)
	}

	// The decoded string should be something like:
	// username:password
	username, password, foundColon := strings.Cut(string(decoded), ":")
	if !foundColon {
		return false
	}

	usernameMatches := (xxhash.Sum64String(username) == usernameHash)
	passwordMatches := (xxhash.Sum64String(password) == passwordHash)

	return usernameMatches && passwordMatches
}

// Use the MAC address as a computer-specific identifier. If it is not available, use the hostname instead.
func GetSpecificIdentifier() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("GetSpecificIdentifier: %v", err)
	}

	for _, iface := range interfaces {
		if len(iface.HardwareAddr.String()) != 0 {
			return iface.HardwareAddr.String(), nil
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", nil
	}

	return hostname, nil
}

func GetDecodedBody(ctx *fasthttp.RequestCtx) ([]byte, error) {
	contentEncoding := string(ctx.Request.Header.Peek("Content-Encoding"))
	switch contentEncoding {
	case "":
		return ctx.Request.Body(), nil
	case "gzip":
		return gunzip(ctx.Request.Body())
	default:
		return nil, fmt.Errorf("GetDecodedBody: unsupported content encoding: %s", contentEncoding)
	}
}

func gunzip(data []byte) ([]byte, error) {
	return fasthttp.AppendGunzipBytes(nil, data)
}

// This takes a group of JSON objects (not inside a JSON array) and splits them
// into individual JSON objects.
func ExtractSeriesOfJsonObjects(body []byte) ([]map[string]interface{}, error) {
	var objects []map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(body))

	for {
		var obj map[string]interface{}
		if err := decoder.Decode(&obj); err != nil {
			// If we reach the end of the input, break the loop.
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("ExtractSeriesOfJsonObjects: error decoding JSON: %v", err)
		}

		objects = append(objects, obj)
	}

	return objects, nil
}

func sendErrorWithStatus(ctx *fasthttp.RequestCtx, messageToUser string, extraMessageToLog string, err error, statusCode int) {
	// Get the caller function name, file name, and line number.
	pc, _, _, _ := runtime.Caller(2) // Get the caller two levels up.
	caller := runtime.FuncForPC(pc)
	callerName := "unknown"
	callerFile := "unknown"
	callerLine := 0

	if caller != nil {
		callerName = caller.Name()
		callerFile, callerLine = caller.FileLine(pc)

		// Only take the function name after the last dot.
		callerName = callerName[strings.LastIndex(callerName, ".")+1:]

		// Only take the /pkg/... part of the file path.
		callerFile = callerFile[strings.LastIndex(callerFile, "/pkg/")+1:]
	}

	// Log the error message.
	if extraMessageToLog == "" {
		log.Errorf("%s at %s:%d: %v, err=%v", callerName, callerFile, callerLine, messageToUser, err)
	} else {
		log.Errorf("%s at %s:%d: %v. %v, err=%v", callerName, callerFile, callerLine, messageToUser, extraMessageToLog, err)
	}

	// Send the error message to the client.
	responsebody := make(map[string]interface{})
	responsebody["error"] = messageToUser
	ctx.SetStatusCode(statusCode)
	WriteJsonResponse(ctx, responsebody)
}

func SendError(ctx *fasthttp.RequestCtx, messageToUser string, extraMessageToLog string, err error) {
	sendErrorWithStatus(ctx, messageToUser, extraMessageToLog, err, fasthttp.StatusBadRequest)
}

func SendInternalError(ctx *fasthttp.RequestCtx, messageToUser string, extraMessageToLog string, err error) {
	sendErrorWithStatus(ctx, messageToUser, extraMessageToLog, err, fasthttp.StatusInternalServerError)
}
