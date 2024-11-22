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

package record

import (
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type MockRRCsReader struct {
	RRCs          []*utils.RecordResultContainer
	FieldToValues map[string][]utils.CValueEnclosure
	ReaderId      utils.T_SegReaderId
}

func (mocker *MockRRCsReader) GetReaderId() utils.T_SegReaderId {
	return mocker.ReaderId
}

func (mocker *MockRRCsReader) ReadAllColsForRRCs(segKey string, vTable string, rrcs []*utils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}) (map[string][]utils.CValueEnclosure, error) {

	columns, err := mocker.GetColsForSegKey(segKey, vTable)
	if err != nil {
		log.Errorf("ReadAllColsForRRCs: cannot get all columns; err=%v", err)
		return nil, err
	}

	for ignoredCol := range ignoredCols {
		delete(columns, ignoredCol)
	}

	results := make(map[string][]utils.CValueEnclosure)
	for cname := range columns {
		values, err := mocker.ReadColForRRCs(segKey, rrcs, cname, qid)
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
	toputils.AddSliceToSet(columns, toputils.GetKeysOfMap(mocker.FieldToValues))

	return columns, nil
}

func (mocker *MockRRCsReader) ReadColForRRCs(_segKey string, rrcs []*utils.RecordResultContainer,
	cname string, _qid uint64) ([]utils.CValueEnclosure, error) {

	if _, ok := mocker.FieldToValues[cname]; !ok {
		return nil, nil
	}

	values := make([]utils.CValueEnclosure, 0, len(rrcs))
Outer:
	for _, rrc := range rrcs {
		for i := range mocker.RRCs {
			if mocker.RRCs[i] == rrc {
				values = append(values, mocker.FieldToValues[cname][i])
				continue Outer
			}
		}

		return nil, toputils.TeeErrorf("ReadColForRRCs: rrc %+v not found", rrc)
	}

	return values, nil
}
