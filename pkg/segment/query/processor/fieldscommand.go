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

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
)

type fieldsProcessor struct {
	options *structs.ColumnsRequest
}

// Return all the columns in finalCols that match any of the wildcardCols,
// which may or may not contain wildcards.
// Note that the results may have duplicates if a column in finalCols matches
// multiple wildcardCols.
func getMatchingColumns(wildcardCols []string, finalCols map[string]struct{}) []string {
	currentCols := make([]string, len(finalCols))
	i := 0
	for col := range finalCols {
		currentCols[i] = col
		i++
	}

	matchingCols := make([]string, 0)
	for _, wildcardCol := range wildcardCols {
		matchingCols = append(matchingCols, utils.SelectMatchingStringsWithWildcard(wildcardCol, currentCols)...)
	}

	return matchingCols
}

func (p *fieldsProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}
	allCnames, err := iqr.GetAllColumnNames()
	if err != nil {
		return nil, fmt.Errorf("fieldsProcessor.Process: cannot get all column names; err=%v", err)
	}

	colsToDelete := make(map[string]struct{})

	// Add excluded columns to deletedColumns
	if p.options.ExcludeColumns != nil {
		matchedCnames := getMatchingColumns(p.options.ExcludeColumns, allCnames)
		for _, cname := range matchedCnames {
			colsToDelete[cname] = struct{}{}
		}
	}

	// Add all the columns except the include columns to deletedColumns
	if p.options.IncludeColumns != nil {
		matchedCnames := getMatchingColumns(p.options.IncludeColumns, allCnames)
		includeCnames := make(map[string]struct{})
		for _, cname := range matchedCnames {
			includeCnames[cname] = struct{}{}
		}
		
		// remove all columns that should not be included
		for cname := range allCnames {
			if _, ok := includeCnames[cname]; !ok {
				colsToDelete[cname] = struct{}{}
			}
		}
	}

	if len(colsToDelete) > 0 {
		err := iqr.DeleteColumns(colsToDelete)
		if err != nil {
			return nil, fmt.Errorf("fieldsProcessor.Process: cannot delete columns; err: %v", err)
		}
	}

	return iqr, nil
}

func (p *fieldsProcessor) Rewind() {
	// do nothing
}

func (p *fieldsProcessor) Cleanup() {
	// do nothing
}
