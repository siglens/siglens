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

package reader

import "github.com/siglens/siglens/pkg/segment/structs"

/*
Checks if this query + aggs is the special Kibana/ES get all indices query.

Returns true if it is the special query, and a string for the special agg name (empty string if not special query)
*/
func isAllIndexAggregationQuery(query *structs.ASTNode, aggs *structs.QueryAggregators, qid uint64) (bool, string) {
	if query != nil {
		return false, ""
	}
	if aggs == nil {
		return false, ""
	}
	if aggs.TimeHistogram != nil {
		return false, ""
	}
	if aggs.GroupByRequest != nil {
		if len(aggs.GroupByRequest.GroupByColumns) == 1 {
			if aggs.GroupByRequest.GroupByColumns[0] == "_index" {
				return true, aggs.GroupByRequest.AggName
			}
		}
	}
	return false, ""
}
