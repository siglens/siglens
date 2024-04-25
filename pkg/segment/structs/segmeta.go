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

package structs

import (
	"fmt"
)

const MAX_SEGMETA_FSIZE = 10_000_000 // 10 MB

type ColSizeInfo struct {
	CmiSize uint64 `json:"cmiSize"`
	CsgSize uint64 `json:"csgSize"`
}

type VtableCounts struct {
	BytesCount       uint64
	RecordCount      uint64
	OnDiskBytesCount uint64
}

type SegMeta struct {
	SegmentKey         string                  `json:"segmentKey"`
	EarliestEpochMS    uint64                  `json:"earliestEpochMs,omitempty"`
	LatestEpochMS      uint64                  `json:"latestEpochMs,omitempty"`
	SegbaseDir         string                  `json:"segbaseDir,omitempty"`
	VirtualTableName   string                  `json:"virtualTableName"`
	RecordCount        int                     `json:"recordCount,omitempty"`
	BytesReceivedCount uint64                  `json:"bytesReceivedCount,omitempty"`
	OnDiskBytes        uint64                  `json:"onDiskBytes,omitempty"`
	ColumnNames        map[string]*ColSizeInfo `json:"columnNames,omitempty"`
	AllPQIDs           map[string]bool         `json:"pqids,omitempty"`
	NumBlocks          uint16                  `json:"numBlocks,omitempty"`
	OrgId              uint64                  `json:"orgid,omitempty"`
}

type MetricsMeta struct {
	MSegmentDir        string          `json:"mSegmentDir"`
	NumBlocks          uint16          `json:"numBlocks"`
	BytesReceivedCount uint64          `json:"bytesReceivedCount"`
	OnDiskBytes        uint64          `json:"onDiskBytes"`
	TagKeys            map[string]bool `json:"tagKeys"`
	EarliestEpochSec   uint32          `json:"earliestEpochSec"`
	LatestEpochSec     uint32          `json:"latestEpochSec"`
	TTreeDir           string          `json:"TTreeDir"`
	DatapointCount     uint64          `json:"approximateDatapointCount"`
	OrgId              uint64          `json:"orgid"`
}

type FileType int

const (
	Cmi    FileType = iota // columnar micro index
	Csg                    // columnar seg file
	Bsu                    // block summary
	Sid                    // segment info
	Pqmr                   // segment persistent query matched results
	Rollup                 // rollup files
	Sst                    // Segment column stats files
)

type SegSetFile struct {
	SegKey string `json:"SegKey"`
	// identifier for file. If csg, xxhash of column name, if for block micro files empty string, if spqmr, then qid
	Identifier string   `json:"Identifier"`
	FileType   FileType `json:"FileType"`
}

type SegSetData struct {
	InUse          bool   `json:"-"` // is this seg file being used? This will tell cleaner if it can delete it or not
	AccessTime     int64  `json:"AccessTime"`
	Size           uint64 `json:"Size"` // size in bytes of file
	SegSetFileName string `json:"SegSetFileName"`
}

type SegSetDataWithStrFname struct {
	Size       uint64 `json:"Size"`
	IsPresent  bool   `json:"IsPresent"`
	AccessTime int64  `json:"AccessTime"`
	LatestTime uint64 `json:"LatestTime"`
	SegSetFile string `json:"SegSetFile"`
}

/*
   ************ Algo for timestamp encoding *****************
   1. As records keep coming in, we add the ts (uint64) to the inProgTss array

   2. Once WIP is full, then instead of writing the full resultion Ts to files,
	  we get the max diff (highTs-lowTs) for this wip block, and then we only store
	  the difference of each Ts from the lowTs in the array.
	  This way we end of using either 2 or 4 bytes per TS instead of 8.

   3. Based on what max diff value is we store the type of TS for this block in "CompTsType"

   4. We then populate the corresponding "CompRecTsXX" array storing only the diffs

   5. During metaloading and query checking, when incoming queryRangeTs is passed,
	  we recompute the actualTs for each record by adding the blockLowTs to the diff
	  of each Ts from the corresponding "CompRecTsXX" array and use it to compare.
	  So basically 2 additional checks traded off for saving 4 bytes for Ts.

   ***********************************************************
*/

const SIZE_OF_BSUM = 18

// If new member is added to BlockSum, make sure to reset it's value in resetwipblock()
type BlockSummary struct {
	HighTs   uint64
	LowTs    uint64
	RecCount uint16
}

const SIZE_OF_BlockInfo = 14 // 2 + 4 + 8 bytes
type BlockInfo struct {
	BlkNum    uint16
	BlkLen    uint32
	BlkOffset int64
	// if you add a element here make sure to update the SIZE_OF_BlockInfo
}

type HostMetaData struct {
	MetaFileName string `json:"metaFileName"`
}

func (bs *BlockSummary) GetSize() uint64 {

	totalSize := uint64(16) // highTS/lowTS
	totalSize += 2          // reccount

	return totalSize
}

// returns a deep copy of the block summary
func (bs *BlockSummary) Copy() *BlockSummary {

	return &BlockSummary{
		HighTs:   bs.HighTs,
		LowTs:    bs.LowTs,
		RecCount: bs.RecCount,
	}
}

type TS_TYPE uint8

const (
	TS_Type8 = iota + 1
	TS_Type16
	TS_Type32
	TS_Type64
)

func GetBsuFnameFromSegKey(segkey string) string {
	return fmt.Sprintf("%s.bsu", segkey)
}
