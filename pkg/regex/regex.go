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

var simpleRe = regexp.MustCompile(`^(\(\?i\))?` + // Optional case-insensitive flag
	`(\^)?` + // Optional anchor
	`(\.\*)?` + // Optional wildcard
	`([a-zA-Z0-9_]+)` + // Main word to find
	`(\.\*)?` + // Optional wildcard
	`(\$)?$`, // Optional anchor
)

func New(pattern string) (Regex, error) {
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
		return contains(buf, r.word)
	}

	if r.wildcardBefore {
		return len(buf) >= len(r.word) && contains(buf[len(buf)-len(r.word):], r.word)
	}

	if r.wildcardAfter {
		return len(buf) >= len(r.word) && contains(buf[:len(r.word)], r.word)
	}

	return equal(buf, r.word)
}

func (r *simpleRegex) String() string {
	return r.fullPattern
}
