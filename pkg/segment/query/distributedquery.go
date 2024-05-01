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

package query

import (
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
)

type DistributedQueryServiceInterface interface {
	Wait(qid uint64, querySummary *summary.QuerySummary) error
	DistributeRotatedRequests(qI *QueryInformation, qsrs []*QuerySegmentRequest) ([]*QuerySegmentRequest, uint64, error)
	DistributeUnrotatedQuery(qI *QueryInformation) (uint64, error)
}

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

func (d *DistributedQueryService) DistributeRotatedRequests(qI *QueryInformation, qsrs []*QuerySegmentRequest) ([]*QuerySegmentRequest, uint64, error) {
	if d == nil {
		return qsrs, 0, nil
	}

	return qsrs, 0, nil
}

func (d *DistributedQueryService) DistributeUnrotatedQuery(qI *QueryInformation) (uint64, error) {
	if d == nil {
		return 0, nil
	}
	return 0, nil
}
