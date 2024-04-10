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
