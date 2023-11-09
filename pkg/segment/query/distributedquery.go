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

package query

import (
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
)

type DistributedQueryService struct {
	isDistributed bool // whether or not this is a distributed query
}

func InitDistQueryService(querySummary *summary.QuerySummary, allSegFileResults *segresults.SearchResults) *DistributedQueryService {

	return &DistributedQueryService{
		isDistributed: false,
	}
}

func (d *DistributedQueryService) Wait(qid uint64, querySummary *summary.QuerySummary) error {
	if d == nil {
		return nil
	}
	if !d.isDistributed {
		return nil
	}
	return nil
}

func (d *DistributedQueryService) DistributeRotatedRequests(qI *queryInformation, qsrs []*querySegmentRequest) ([]*querySegmentRequest, uint64, error) {
	if d == nil {
		return qsrs, 0, nil
	}

	return qsrs, 0, nil
}

func (d *DistributedQueryService) DistributeUnrotatedQuery(qI *queryInformation) (uint64, error) {
	if d == nil {
		return 0, nil
	}
	return 0, nil
}
