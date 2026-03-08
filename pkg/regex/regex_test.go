// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

	assertMatches(t, `.*google.*`, "My favorite search engine is google")
	assertMatches(t, `.*google.*`, "Google with different case")
	assertMatches(t, `(?i).*google.*`, "GOOGLE with case insensitivity")
	assertMatches(t, `.*google.*`, "My\nfavorite\nsearch\nengine\nis\ngoogle")
	assertMatches(t, `.*search.*google.*`, "I want to search\nusing google")
	assertMatches(t, `.*google.*search.*`, "Using google to\nsearch the web")

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
func Test_simpleRegex_Match(t *testing.T) {
	tests := []struct {
		name          string
		pattern       string
		input         string
		shouldMatch   bool
		caseSensitive bool
	}{
		{
			name:        "exact match, case sensitive",
			pattern:     "foo",
			input:       "foo",
			shouldMatch: true,
		},
		{
			name:        "exact match, case sensitive, mismatch",
			pattern:     "foo",
			input:       "Foo",
			shouldMatch: false,
		},
		{
			name:        "exact match, case insensitive",
			pattern:     "(?i)foo",
			input:       "Foo",
			shouldMatch: true,
		},
		{
			name:        "wildcard before and after, contains",
			pattern:     ".*bar.*",
			input:       "xxbarxx",
			shouldMatch: true,
		},
		{
			name:        "wildcard before and after, does not contain",
			pattern:     ".*bar.*",
			input:       "baz",
			shouldMatch: false,
		},
		{
			name:        "wildcard before only, ends with",
			pattern:     ".*end",
			input:       "theend",
			shouldMatch: true,
		},
		{
			name:        "case insensitive ends with",
			pattern:     "(?i).*BAR",
			input:       "abcBaR",
			shouldMatch: true,
		},
		{
			name:        "case insensitive starts with",
			pattern:     "(?i)foo.*",
			input:       "FOObar",
			shouldMatch: true,
		},
		{
			name:        "exact match, case insensitive",
			pattern:     "(?i)foo",
			input:       "fOo",
			shouldMatch: true,
		},
		{
			name:        "exact match, case insensitive, mismatch",
			pattern:     "(?i)foo",
			input:       "bar",
			shouldMatch: false,
		},
		{
			name:        "wildcard before and after, case insensitive, does not contain",
			pattern:     "(?i).*bar.*",
			input:       "baz",
			shouldMatch: false,
		},
		{
			name:        "pattern with allowed special chars",
			pattern:     "foo-bar_123:/",
			input:       "foo-bar_123:/",
			shouldMatch: true,
		},
		{
			name:        "pattern with allowed special chars, mismatch",
			pattern:     "foo-bar_123:/",
			input:       "foo-bar_123",
			shouldMatch: false,
		},
		{
			name:        "multiline text with wildcard match",
			pattern:     `.*google.*`,
			input:       `watching\nhttps://google.com`,
			shouldMatch: true,
		},
		{
			name:        "multiline text with case insensitive wildcard match",
			pattern:     `(?i).*GOOGLE.*`,
			input:       `watching\nhttps://google.com/search`,
			shouldMatch: true,
		},
		{
			name:        "multiline text with no match",
			pattern:     `.*bing.*`,
			input:       `watching\nhttps://google.com`,
			shouldMatch: false,
		},
		{
			name:        "multiline text with start anchor",
			pattern:     `^watching.*`,
			input:       `watching\nhttps://google.com`,
			shouldMatch: true,
		},
		{
			name:        "multiline text with end anchor",
			pattern:     `.*com$`,
			input:       `watching\nhttps://google.com`,
			shouldMatch: true,
		},
		{
			name:        "multiline text spanning multiple lines",
			pattern:     `.*watching.*com.*`,
			input:       `user is watching\nhttps://google.com\nright now`,
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := regexp.MustCompile(tt.pattern).MatchString(tt.input)
			if matched != tt.shouldMatch {
				t.Errorf("expected match: %v, got: %v", tt.shouldMatch, matched)
			}
		})
	}
}

func Test_containsSingleDotWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		want    bool
	}{
		{`foo.bar`, true},
		{`foo.*bar`, false},
		{`foo\.bar`, false},   // Escaped dot
		{`foo\\.bar`, true},   // Escaped backslash followed by dot
		{`foo\\\.bar`, false}, // Escaped backslash followed by escaped dot
		{`foo.bar.*`, true},
		{`foo.*`, false},
		{`foo`, false},
		{`foo\.*bar`, false},
		{`foo.*bar.*baz`, false},
		{`foo.bar.baz`, true},
		{`foo\..*bar`, false},
		{`foo\\\\.bar`, true},   // Two escaped backslashes followed by dot
		{`foo\\\\\.bar`, false}, // Two escaped backslashes followed by escaped dot
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := containsSingleDotWildcard(tt.pattern)
			assert.Equal(t, tt.want, got, "Pattern: %s", tt.pattern)
		})
	}
}

func Test_simpleRegex_String(t *testing.T) {
	reg, err := New("foo.*")
	assert.NoError(t, err)
	assert.Equal(t, "foo.*", reg.String())
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
