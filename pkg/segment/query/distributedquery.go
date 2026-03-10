// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package query

import (
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
)

type DistributedQueryServiceInterface interface {
	Wait(qid uint64, querySummary *summary.QuerySummary) error
	DistributeQuery(qI *QueryInformation) (uint64, error)
	IsDistributed() bool
	GetSegEncToKeyBaseValue() uint32
	GetNumNodesDistributedTo() uint64
	GetNumRemoteRecordsToSearch() uint64
	GetOwnedSegments(orgId int64) (map[string]struct{}, error)
}

type DistributedQueryService struct {
	isDistributed bool // whether or not this is a distributed query
}

func InitDistQueryService(querySummary *summary.QuerySummary, allSegFileResults *segresults.SearchResults, dqid string, segEncTokey uint32) *DistributedQueryService {

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

func (d *DistributedQueryService) DistributeQuery(qI *QueryInformation) (uint64, error) {
	if d == nil {
		return 0, nil
	}
	return 0, nil
}

func (d *DistributedQueryService) IsDistributed() bool {
	if d == nil {
		return false
	}
	return d.isDistributed
}

func (d *DistributedQueryService) GetSegEncToKeyBaseValue() uint32 {
	return 0
}

func (d *DistributedQueryService) GetNumNodesDistributedTo() uint64 {
	return 0
}

func (d *DistributedQueryService) GetNumRemoteRecordsToSearch() uint64 {
	return 0
}

func (d *DistributedQueryService) GetOwnedSegments(orgId int64) (map[string]struct{}, error) {
	return metadata.GetAllSegKeysForOrg(orgId), nil
}
