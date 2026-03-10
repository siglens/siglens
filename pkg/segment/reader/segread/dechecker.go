// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package segread

import (
	"fmt"
	"regexp"

	"github.com/siglens/siglens/pkg/segment/reader/segread/segreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
)

var ErrNilRegex = fmt.Errorf("got nil regex")

/*
parameters:

	matchFilter : input query mf
	dictData: mapping of dict keywords --> raw recNums buf slice

returns:

	bool: if there is a match
	err
*/
func ApplySearchToMatchFilterDictCsg(sfr *segreader.SegmentFileReader, match *structs.MatchFilter,
	bsh *structs.BlockSearchHelper, isCaseInsensitive bool) (bool, error) {
	var compiledRegex *regexp.Regexp
	var err error

	if len(match.MatchWords) == 0 {
		return false, nil
	}

	if match.MatchType == structs.MATCH_PHRASE {
		compiledRegex, err = match.GetRegexp()
		if err != nil {
			return false, ErrNilRegex
		}
	}

	anyMatched := false
	for dwordIdx, dWord := range sfr.GetDeTlv() {
		matched, err := writer.ApplySearchToMatchFilterRawCsg(match, dWord, compiledRegex, isCaseInsensitive)
		if err != nil {
			return false, err
		}
		if matched {
			anyMatched = true
			sfr.AddRecNumsToMr(uint16(dwordIdx), bsh)
		}
	}

	return anyMatched, nil
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
func ApplySearchToExpressionFilterDictCsg(sfr *segreader.SegmentFileReader, qValDte *sutils.DtypeEnclosure,
	fop sutils.FilterOperator, isRegexSearch bool, bsh *structs.BlockSearchHelper, isCaseInsensitive bool) (bool, error) {

	if qValDte == nil {
		return false, nil
	}

	if isRegexSearch && qValDte.GetRegexp() == nil {
		return false, ErrNilRegex
	}

	dte := &sutils.DtypeEnclosure{}
	anyMatched := false
	for dwordIdx, dWord := range sfr.GetDeTlv() {
		matched, err := writer.ApplySearchToExpressionFilterSimpleCsg(qValDte, fop, dWord, isRegexSearch, dte, isCaseInsensitive)
		if err != nil {
			return false, err
		}
		if matched {
			anyMatched = true
			sfr.AddRecNumsToMr(uint16(dwordIdx), bsh)
		}
	}

	return anyMatched, nil
}
