// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type MockRRCsReader struct {
	RRCs          []*sutils.RecordResultContainer
	FieldToValues map[string][]sutils.CValueEnclosure
	ReaderId      sutils.T_SegReaderId
}

func (mocker *MockRRCsReader) GetReaderId() sutils.T_SegReaderId {
	return mocker.ReaderId
}

func (mocker *MockRRCsReader) ReadAllColsForRRCs(segKey string, vTable string, rrcs []*sutils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}) (map[string][]sutils.CValueEnclosure, error) {

	columns, err := mocker.GetColsForSegKey(segKey, vTable)
	if err != nil {
		log.Errorf("ReadAllColsForRRCs: cannot get all columns; err=%v", err)
		return nil, err
	}

	for ignoredCol := range ignoredCols {
		delete(columns, ignoredCol)
	}

	results := make(map[string][]sutils.CValueEnclosure)
	for cname := range columns {
		values, err := mocker.ReadColForRRCs(segKey, rrcs, cname, qid, false)
		if err != nil {
			log.Errorf("ReadAllColsForRRCs: cannot read column %v; err=%v", cname, err)
			return nil, err
		}

		results[cname] = values
	}

	return results, nil
}

func (mocker *MockRRCsReader) GetColsForSegKey(_segKey string, _vTable string) (map[string]struct{}, error) {
	columns := make(map[string]struct{})
	utils.AddSliceToSet(columns, utils.GetKeysOfMap(mocker.FieldToValues))

	return columns, nil
}

func (mocker *MockRRCsReader) ReadColForRRCs(_segKey string, rrcs []*sutils.RecordResultContainer,
	cname string, _qid uint64, _fetchFromBlob bool) ([]sutils.CValueEnclosure, error) {

	if _, ok := mocker.FieldToValues[cname]; !ok {
		return nil, nil
	}

	values := make([]sutils.CValueEnclosure, 0, len(rrcs))
Outer:
	for _, rrc := range rrcs {
		for i := range mocker.RRCs {
			if mocker.RRCs[i] == rrc {
				values = append(values, mocker.FieldToValues[cname][i])
				continue Outer
			}
		}

		return nil, utils.TeeErrorf("ReadColForRRCs: rrc %+v not found", rrc)
	}

	return values, nil
}
