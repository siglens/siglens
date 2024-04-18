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

package utils

import (
	"regexp"
	"strings"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	log "github.com/sirupsen/logrus"
)

// Converts a string like `This has "quotes"` to `This has \"quotes\"`
func EscapeQuotes(s string) string {
	result := ""
	for _, ch := range s {
		if ch == '"' {
			result += "\\"
		}

		result += string(ch)
	}

	return result
}

// Return all strings in `slice` that match `s`, which may have wildcards.
func SelectMatchingStringsWithWildcard(s string, slice []string) []string {
	if strings.Contains(s, "*") {
		s = dtypeutils.ReplaceWildcardStarWithRegex(s)
	}

	// We only want exact matches.
	s = "^" + s + "$"

	compiledRegex, err := regexp.Compile(s)
	if err != nil {
		log.Errorf("SelectMatchingStringsWithWildcard: regex compile failed: %v", err)
		return nil
	}

	matches := make([]string, 0)
	for _, potentialMatch := range slice {
		if compiledRegex.MatchString(potentialMatch) {
			matches = append(matches, potentialMatch)
		}
	}

	return matches
}
