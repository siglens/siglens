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
