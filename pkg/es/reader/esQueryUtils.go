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
