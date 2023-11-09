/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
