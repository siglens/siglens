// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
