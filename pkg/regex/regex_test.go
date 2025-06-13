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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Match(t *testing.T) {
	assertMatches(t, `.*`, `abc`)
	assertMatches(t, `.*foo.*`, `abc`)
	assertMatches(t, `.*foo.*`, `foo`)
	assertMatches(t, `.*foo.*`, `abcfooxyz`)
	assertMatches(t, `foo.*`, `foobar`)
	assertMatches(t, `foo.*`, `abcfooxyz`)
	assertMatches(t, `^foo.*`, `abcfooxyz`)
	assertMatches(t, `^.*foo.*`, `abcfooxyz`)
	assertMatches(t, `(?i).*bar$`, `abcBaR`)
	assertMatches(t, `.*bar$`, `abcBaR`)

	assertMatches(t, `.*google.*`, "visit\nwww.google.com")
	assertMatches(t, `(?i).*GOOGLE.*`, "some\ntext\nGoogle\nhere")
	assertMatches(t, `.*foo.*`, "line1\nline2fooinfo")

}

func assertMatches(t *testing.T, pattern string, str string) {
	t.Helper()

	regex, err := New(pattern)
	assert.NoError(t, err)

	actualRegex, err := regexp.Compile(pattern)
	assert.NoError(t, err)
	shouldMatch := actualRegex.Match([]byte(str))

	if shouldMatch {
		assert.True(t, regex.Match([]byte(str)), "Pattern %s should match %s", pattern, str)
	} else {
		assert.False(t, regex.Match([]byte(str)), "Pattern %s should not match %s", pattern, str)
	}
}

func Test_UsesOptimizedRegex(t *testing.T) {
	assertUsesOptimizedRegex(t, `.*`, false)
	assertUsesOptimizedRegex(t, `.*foo.*`, true)
	assertUsesOptimizedRegex(t, `foo.*`, true)
	assertUsesOptimizedRegex(t, `^foo.*`, true)
	assertUsesOptimizedRegex(t, `^.*foo.*`, true)
	assertUsesOptimizedRegex(t, `^.*foo$`, true)
	assertUsesOptimizedRegex(t, `^.*foo.*$`, true)
	assertUsesOptimizedRegex(t, `^foo$`, true)
	assertUsesOptimizedRegex(t, `(?i).*foo$`, true)

	assertUsesOptimizedRegex(t, `foo.*bar.*`, false) // TODO: maybe we'll want to handle this.
	assertUsesOptimizedRegex(t, `(.*foo.*|.*bar.*)`, false)

}

func assertUsesOptimizedRegex(t *testing.T, pattern string, expected bool) {
	t.Helper()

	regex, err := New(pattern)
	assert.NoError(t, err)

	_, ok := regex.(*simpleRegex)
	if expected {
		assert.True(t, ok, "Pattern %s should use optimized regex", pattern)
	} else {
		assert.False(t, ok, "Pattern %s should not use optimized regex", pattern)
	}
}
