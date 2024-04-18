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

package structs

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MatchFilter_GetRegexp(t *testing.T) {
	// Test cases
	tests := []struct {
		name          string
		filter        MatchFilter
		expectedRegex *regexp.Regexp
		shouldError   bool
	}{
		{
			name: "empty regexp",
			filter: MatchFilter{
				RegexpString: "",
			},
			expectedRegex: nil,
			shouldError:   false,
		},
		{
			name: "invalid regexp",
			filter: MatchFilter{
				RegexpString: "[a-z",
			},
			expectedRegex: nil,
			shouldError:   true,
		},
		{
			name: "valid regexp",
			filter: MatchFilter{
				RegexpString: "[a-z]",
			},
			expectedRegex: regexp.MustCompile("[a-z]"),
			shouldError:   false,
		},
	}

	// Run the tests.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re, err := tt.filter.GetRegexp()
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedRegex, re)
			}
		})
	}
}
