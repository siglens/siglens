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

package processor

import (
	"fmt"
	"io"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
)

type fieldsProcessor struct {
	options *structs.ColumnsRequest
}

// Return all the columns in finalCols that match any of the wildcardCols
// along with the first index of the match
func getMatchingColumns(wildcardCols []string, finalCols map[string]struct{}) map[string]int {
	currentCols := make([]string, len(finalCols))
	for col := range finalCols {
		currentCols = append(currentCols, col)
	}

	matchedCnames := make(map[string]int)

	for idx, wildcardCol := range wildcardCols {
		matchedCols := utils.SelectMatchingStringsWithWildcard(wildcardCol, currentCols)
		for _, col := range matchedCols {
			if _, ok := matchedCnames[col]; !ok {
				matchedCnames[col] = idx
			}
		}
	}

	return matchedCnames
}

func (p *fieldsProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}
	allCnames, err := iqr.GetColumns()
	if err != nil {
		return nil, fmt.Errorf("fieldsProcessor.Process: cannot get all column names, err: %v", err)
	}

	colsToDelete := make(map[string]struct{})
	colsIndex := make(map[string]int)

	// Add excluded columns to deletedColumns
	if p.options.ExcludeColumns != nil {
		matchedCnames := getMatchingColumns(p.options.ExcludeColumns, allCnames)
		for cname := range matchedCnames {
			colsToDelete[cname] = struct{}{}
		}
	}

	// Add all the columns except the include columns to deletedColumns
	if p.options.IncludeColumns != nil {
		matchedCnames := getMatchingColumns(p.options.IncludeColumns, allCnames)
		includeCnames := make(map[string]struct{})
		// By default, the timestamp column is always included.
		// Check SPL Fields cmd reference: https://docs.splunk.com/Documentation/Splunk/9.3.1/SearchReference/Fields
		includeCnames[config.GetTimeStampKey()] = struct{}{}
		colsIndex[config.GetTimeStampKey()] = 0
		for cname, index := range matchedCnames {
			includeCnames[cname] = struct{}{}
			colsIndex[cname] = index + 1
		}

		// remove all columns that should not be included
		for cname := range allCnames {
			if _, ok := includeCnames[cname]; !ok {
				colsToDelete[cname] = struct{}{}
			}
		}
	}

	iqr.AddColumnsToDelete(colsToDelete)
	iqr.AddColumnIndex(colsIndex)

	return iqr, nil
}

func (p *fieldsProcessor) Rewind() {
	// do nothing
}

func (p *fieldsProcessor) Cleanup() {
	// do nothing
}

func (p *fieldsProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}
