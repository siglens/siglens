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

package segread

import (
	"errors"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
)

/*
parameters:

	matchFilter : input query mf
	dictData: mapping of dict keywords --> raw recNums buf slice

returns:

	bool: if there is a match
	err
*/
func (sfr *SegmentFileReader) ApplySearchToMatchFilterDictCsg(match *structs.MatchFilter,
	bsh *structs.BlockSearchHelper) (bool, error) {

	if len(match.MatchWords) == 0 {
		return false, nil
	}

	for dwordIdx, dWord := range sfr.deTlv {
		matched, err := writer.ApplySearchToMatchFilterRawCsg(match, dWord)
		if err != nil {
			return false, err
		}
		if matched {
			addRecNumsToMr(uint16(dwordIdx), bsh, sfr)
		}
	}

	return false, nil
}

/*
parameters:

	DtypeEnclosure : input qVal
	FilterOperator: filter operator
	isRegexSearch:
	dictData: mapping of dict keywords --> raw recNums buf slice

returns:

	bool: if there is a match
	err
*/
func (sfr *SegmentFileReader) ApplySearchToExpressionFilterDictCsg(qValDte *utils.DtypeEnclosure,
	fop utils.FilterOperator, isRegexSearch bool, bsh *structs.BlockSearchHelper) (bool, error) {

	if qValDte == nil {
		return false, nil
	}

	if isRegexSearch && qValDte.GetRegexp() == nil {
		return false, errors.New("qValDte had nil regexp compilation")
	}

	dte := &utils.DtypeEnclosure{}
	for dwordIdx, dWord := range sfr.deTlv {
		matched, err := writer.ApplySearchToExpressionFilterSimpleCsg(qValDte, fop, dWord, isRegexSearch, dte)
		if err != nil {
			return false, err
		}
		if matched {
			addRecNumsToMr(uint16(dwordIdx), bsh, sfr)
		}
	}

	return false, nil
}

func addRecNumsToMr(dwordIdx uint16, bsh *structs.BlockSearchHelper, sfr *SegmentFileReader) {

	for i := uint16(0); i < sfr.blockSummaries[sfr.currBlockNum].RecCount; i++ {
		if sfr.deRecToTlv[i] == dwordIdx {
			bsh.AddMatchedRecord(uint(i))
		}
	}
}
