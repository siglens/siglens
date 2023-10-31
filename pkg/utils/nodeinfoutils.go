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

package utils

type NodeInfo struct {
	IP                         string `json:"ip"`
	BuildHash                  string `json:"build_hash"`
	TotalIndexingBuffer        int64  `json:"total_indexing_buffer"`
	TotalIndexingBufferInBytes string `json:"total_indexing_buffer_in_bytes"`
	Version                    string `json:"version"`
	HttpPublishAddress         string `json:"http.publish_address"`
}

type IndexInfo struct {
	Name            string `json:"index"`
	Health          string `json:"health"`
	Status          string `json:"status"`
	UUID            string `json:"uuid"`
	Rep             int    `json:"rep"`
	Pri             uint64 `json:"pri"`
	DocsDeleted     int    `json:"docs.deleted"`
	DocsCount       int    `json:"docs.count"`
	StoreSize       uint64 `json:"store.size"`
	OnDiskStoreSize uint64 `json:"store.onDiskBytes"`
	PriStoreSize    uint64 `json:"pri.store.size"`
}

type NodeStatusEnum int

const (
	GREEN NodeStatusEnum = iota
	YELLOW
	RED
	UNKNOWN_NODESTATUS
)

func (s NodeStatusEnum) String() string {
	switch s {
	case GREEN:
		return "GREEN"
	case YELLOW:
		return "YELLOW"
	case RED:
		return "RED"

	default:
		return "UNKNOWN_NODESTATUS"
	}
}

type NodeTypeEnum int

const (
	INGEST NodeTypeEnum = iota
	QUERY
	SEED
	SEGREADER
	METAREADER
)

func (s NodeTypeEnum) String() string {
	switch s {
	case INGEST:
		return "INGEST"
	case QUERY:
		return "QUERY"
	case SEED:
		return "SEED"
	case SEGREADER:
		return "SEGREADER"
	case METAREADER:
		return "METAREADER"

	default:
		return "UNKNOWN_NODETYPE"
	}
}

type ClusterNode struct {
	NodeIP     string         `json:"NodeIp"`
	RaftPort   string         `json:"RaftPort"`
	GRPCPort   string         `json:"GRPCPort"`
	NodeTypes  []NodeTypeEnum `json:"NodeTypes"`
	NodeStatus string         `json:"NodeStatus"`
}

type ClusterNodeStr struct {
	NodeIP     string   `json:"NodeIp"`
	RaftPort   string   `json:"RaftPort"`
	GRPCPort   string   `json:"GRPCPort"`
	NodeTypes  []string `json:"NodeTypes"`
	NodeStatus string   `json:"NodeStatus"`
}

type MemberInfo struct {
	ClusterId      uint64                     `json:"ClusterId"`
	ConfigChangeId uint64                     `json:"ConfigChangeId"`
	Nodes          map[uint64]*ClusterNodeStr `json:"ClusterNode"`
	Observers      map[uint64]string          `json:"Observers"`
	Witnesses      map[uint64]string          `json:"Witnesses"`
	Removed        map[uint64]struct{}        `json:"Removed"`
	LeaderId       uint64                     `json:"LeaderId"`
	LeaderValid    bool                       `json:"LeaderValid"`
}

func NewNodeInfo(esVersion string) *NodeInfo {
	return &NodeInfo{
		IP:                         "127.0.0.1",
		BuildHash:                  "123456",
		TotalIndexingBuffer:        123456,
		TotalIndexingBufferInBytes: "123456",
		// todo get this version from config
		Version:            esVersion,
		HttpPublishAddress: "127.0.0.1:8081",
	}
}

func GetAllNodesInfo(hostname string, esVersion string) map[string]*NodeInfo {

	allNodesInfo := make(map[string]*NodeInfo)
	allNodesInfo[hostname] = NewNodeInfo(esVersion)
	return allNodesInfo
}

func CreateIndexInfo(name string, uuid string, docsCount int, storeSize uint64, onDiskBytesCount uint64) *IndexInfo {
	// TODO: Implement docsCount & storeSize

	// constant values:
	health := "green"
	docs_deleted := 0
	status := ""
	rep := 0

	return &IndexInfo{
		Name:            name,
		Health:          health,
		UUID:            uuid,
		Status:          status,
		Pri:             1,
		Rep:             rep,
		DocsCount:       docsCount,
		DocsDeleted:     docs_deleted,
		StoreSize:       storeSize,
		OnDiskStoreSize: onDiskBytesCount,
		PriStoreSize:    storeSize,
	}
}
