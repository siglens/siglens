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

package processor

import (
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
)

type QueryType uint8

const (
	InvalidQueryType QueryType = iota
	RecordsQuery
	StatsQuery
)

type QueryResult struct {
	DataProcessor
}

func NewQueryResultProcessor(queryType structs.QueryType) (*QueryResult, error) {
	var limit uint64
	switch queryType {
	case structs.RRCCmd:
		limit = utils.QUERY_EARLY_EXIT_LIMIT
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		limit = utils.QUERY_MAX_BUCKETS
	default:
		return nil, toputils.TeeErrorf("NewQueryResultProcessor: invalid query type %v", queryType)
	}

	return &QueryResult{
		DataProcessor: NewHeadDP(toputils.NewOptionWithValue(limit)),
	}, nil
}
