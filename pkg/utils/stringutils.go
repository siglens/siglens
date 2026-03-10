// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"encoding/base64"
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
		log.Errorf("SelectMatchingStringsWithWildcard: regex compile failed, pattern: %v, err: %v", s, err)
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

func EncodeToBase64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func DecodeFromBase64(s string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func MightBeFloat(s string) bool {
	if len(s) == 0 {
		return false
	}

	switch s {
	case "NaN", "nan", "Inf", "inf", "-Inf", "-inf":
		return true
	}

	for _, ch := range s {
		if (ch < '0' || ch > '9') && !strings.ContainsRune(".-+eE", ch) {
			return false
		}
	}

	return true
}
