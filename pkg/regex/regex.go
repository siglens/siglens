// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package regex

import (
	"bytes"
	"regexp"

	"github.com/siglens/siglens/pkg/utils"
)

type Regex interface {
	Match([]byte) bool
	String() string
}

type simpleRegex struct {
	wildcardBefore bool
	word           []byte
	caseSensitive  bool
	wildcardAfter  bool
	fullPattern    string
}

var simpleRe = regexp.MustCompile(
	`^` + // Start of string
		`(\(\?i\))?` + // Optional case-insensitive flag
		`(\^)?` + // Optional start anchor
		`(\.\*)?` + // Optional leading wildcard
		`([a-zA-Z0-9_:/\-]+)` + // The main word (allowed chars)
		`(\.\*)?` + // Optional trailing wildcard
		`(\$)?` + // Optional end anchor
		`$`, // End of string
)

func New(pattern string) (Regex, error) {
	if bytes.Contains([]byte(pattern), []byte("\n")) {
		return regexp.Compile(pattern)
	}

	if containsSingleDotWildcard(pattern) {
		return regexp.Compile(pattern)
	}

	matches := simpleRe.FindStringSubmatch(pattern)
	if len(matches) == 0 {
		return regexp.Compile(pattern)
	}

	caseSensitive := matches[1] != "(?i)"
	word := []byte(matches[4])

	hasStartAnchor := matches[2] == "^"
	hasEndAnchor := matches[6] == "$"
	leadingWildcard := matches[3] == ".*"
	trailingWildcard := matches[5] == ".*"

	wildcardBefore := leadingWildcard || !hasStartAnchor
	wildcardAfter := trailingWildcard || !hasEndAnchor

	return &simpleRegex{
		caseSensitive:  caseSensitive,
		wildcardBefore: wildcardBefore,
		word:           word,
		wildcardAfter:  wildcardAfter,
		fullPattern:    pattern,
	}, nil
}

func containsSingleDotWildcard(pattern string) bool {
	escaped := false
	for i := 0; i < len(pattern); i++ {
		char := pattern[i]

		if escaped {
			escaped = false
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if char == '.' {
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				continue
			}
			return true
		}
	}
	return false
}

func (r *simpleRegex) Match(buf []byte) bool {
	var contains func([]byte, []byte) bool
	var equal func([]byte, []byte) bool
	var hasSuffix func([]byte, []byte) bool
	var hasPrefix func([]byte, []byte) bool

	if r.caseSensitive {
		contains = bytes.Contains
		equal = bytes.Equal
		hasSuffix = bytes.HasSuffix
		hasPrefix = bytes.HasPrefix
	} else {
		contains = utils.ContainsAnyCase
		equal = bytes.EqualFold
		hasSuffix = func(s, suffix []byte) bool {
			if len(s) < len(suffix) {
				return false
			}
			return bytes.EqualFold(s[len(s)-len(suffix):], suffix)
		}
		hasPrefix = func(s, prefix []byte) bool {
			if len(s) < len(prefix) {
				return false
			}
			return bytes.EqualFold(s[:len(prefix)], prefix)
		}
	}

	switch {
	case r.wildcardBefore && r.wildcardAfter:
		return contains(buf, r.word)

	case r.wildcardBefore && !r.wildcardAfter:
		return hasSuffix(buf, r.word)

	case !r.wildcardBefore && r.wildcardAfter:
		return hasPrefix(buf, r.word)

	default:
		return equal(buf, r.word)
	}
}

func (r *simpleRegex) String() string {
	return r.fullPattern
}
