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

package iqr

import (
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Record struct {
	iqr   *IQR
	index int
}

func (record *Record) ReadColumn(cname string) (*utils.CValueEnclosure, error) {
	values, err := record.iqr.ReadColumn(cname)
	if err != nil {
		log.Errorf("Record.ReadColumn: cannot read column %v from IQR; err=%v", cname, err)
		return nil, err
	}

	if record.index >= len(values) {
		err := toputils.TeeErrorf("Record.ReadColumn: index %v out of range (len is %v) for column %v; iqr has %v records",
			record.index, len(values), cname, record.iqr.NumberOfRecords())
		return nil, err
	}

	return &values[record.index], nil
}

func (record *Record) GetTimestamp() (uint64, error) {
	if record.index >= len(record.iqr.rrcs) {
		values, err := record.iqr.ReadColumn("timestamp")
		if err != nil {
			return 0, toputils.TeeErrorf("Record.GetTimestamp: cannot read timestamp column from IQR or from rrcs; err=%v", err)
		}

		if record.index >= len(values) {
			return 0, toputils.TeeErrorf("Record.GetTimestamp: index %v out of range (len is %v) for column timestamp; iqr has %v records",
				record.index, len(values), record.iqr.NumberOfRecords())
		}

		return values[record.index].CVal.(uint64), nil
	}

	rrc := record.iqr.rrcs[record.index]
	return rrc.TimeStamp, nil
}
