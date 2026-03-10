// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
