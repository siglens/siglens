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
	
	allCnames, err := iqr.GetAllColumnNames()
	if err != nil {
		return nil, fmt.Errorf("fieldsProcessor.Process: cannot get all column names; err=%v", err)
	}

	

	if p.options.ExcludeColumns != nil {
		// Remove the specified columns, which may have wildcards.
		matchingCols := getMatchingColumns(p.options.ExcludeColumns, finalCols)
		for _, matchingCol := range matchingCols {
			delete(finalCols, matchingCol)
		}
	}

	if p.options.IncludeColumns != nil {
		// Remove all columns except the specified ones, which may have wildcards.
		if finalCols == nil {
			return errors.New("performColumnsRequest: finalCols is nil")
		}

		matchingCols := getMatchingColumns(p.options.IncludeColumns, finalCols)

		// First remove everything.
		for col := range finalCols {
			delete(finalCols, col)
		}

		// Add the matching columns.
		for index, matchingCol := range matchingCols {
			finalCols[matchingCol] = true
			nodeResult.ColumnsOrder[matchingCol] = index
		}
	}
}

func (p *fieldsProcessor) Rewind() {
	panic("not implemented")
}

func (p *fieldsProcessor) Cleanup() {
	panic("not implemented")
}
