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
