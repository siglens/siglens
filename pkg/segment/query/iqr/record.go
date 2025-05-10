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
	"fmt"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
)

type Record struct {
	iqr       *IQR
	Index     int
	validated bool

	// Outer slice is one column. Only "index" element of inner slice is
	// relevant for this record.
	SortValues [][]sutils.CValueEnclosure
}

func (record *Record) ReadColumn(cname string) (*sutils.CValueEnclosure, error) {
	var values []sutils.CValueEnclosure
	var err error
	if !record.validated {
		values, err = record.iqr.ReadColumn(cname)
		if err == nil {
			record.validated = true
		}
	} else {
		values, err = record.iqr.readColumnInternal(cname)
	}
	if err != nil {
		return nil, fmt.Errorf("Record.ReadColumn: cannot read column %v from IQR; err=%v", cname, err)
	}

	if record.Index >= len(values) {
		return nil, fmt.Errorf("Record.ReadColumn: index %v out of range (len is %v) for column %v; iqr has %v records",
			record.Index, len(values), cname, record.iqr.NumberOfRecords())
	}

	return &values[record.Index], nil
}
