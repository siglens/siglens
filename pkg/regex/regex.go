// Copyright (c) 2021-2025 SigScalr, Inc.
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

	fullPattern string
}

var simpleRe = regexp.MustCompile(`^(\(\?i\))?(\^)?(\.\*)?([a-zA-Z0-9_:/.\-]+)(\.\*)?(\$)?$`)

func New(pattern string) (Regex, error) {
	// fallback if pattern contains newline — multiline match not supported by simpleRegex
	if bytes.Contains([]byte(pattern), []byte("\n")) {
		return regexp.Compile(pattern)
	}

	matches := simpleRe.FindStringSubmatch(pattern)
	if len(matches) == 0 {
		return regexp.Compile(pattern)
	}

	return &simpleRegex{
		caseSensitive:  matches[1] != "(?i)",
		wildcardBefore: matches[2] != "^" || matches[3] == ".*",
		word:           []byte(matches[4]),
		wildcardAfter:  matches[5] == ".*" || matches[6] != "$",
		fullPattern:    pattern,
	}, nil
}

func (r *simpleRegex) Match(buf []byte) bool {
	// Normalize line breaks to a space for matching
	normalized := bytes.ReplaceAll(buf, []byte("\n"), []byte(" "))

	var contains func([]byte, []byte) bool
	var equal func([]byte, []byte) bool
	if r.caseSensitive {
		contains = bytes.Contains
		equal = bytes.Equal
	} else {
		contains = utils.ContainsAnyCase
		equal = bytes.EqualFold
	}

	if r.wildcardBefore && r.wildcardAfter {
		return contains(normalized, r.word)
	}

	if r.wildcardBefore {
		return len(normalized) >= len(r.word) && contains(normalized[len(normalized)-len(r.word):], r.word)
	}

	if r.wildcardAfter {
		return len(normalized) >= len(r.word) && contains(normalized[:len(r.word)], r.word)
	}

	return equal(normalized, r.word)
}

func (r *simpleRegex) String() string {
	return r.fullPattern
}
