package health

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/stretchr/testify/assert"
)

func TestParseIngestionStatsRequest(t *testing.T) {
	defaultPastHours := uint64(7 * 24) // 7 days

	tests := []struct {
		name          string
		jsonInput     map[string]interface{}
		expectedHours uint64
		expectedGran  usageStats.UsageStatsGranularity
		description   string
		timeRelative  bool
	}{
		{
			name:          "Empty JSON",
			jsonInput:     map[string]interface{}{},
			expectedHours: defaultPastHours,
			expectedGran:  usageStats.Daily,
			description:   "Should return default values when JSON is empty",
			timeRelative:  false,
		},
		{
			name: "Only granularity specified",
			jsonInput: map[string]interface{}{
				"granularity": "hourly",
			},
			expectedHours: defaultPastHours,
			expectedGran:  usageStats.Hourly,
			description:   "Should use provided granularity but default hours",
			timeRelative:  false,
		},
		{
			name: "Valid timestamps with minute granularity",
			jsonInput: map[string]interface{}{
				"startEpoch":  time.Now().Add(-6 * time.Hour).Unix(),
				"endEpoch":    time.Now().Unix(),
				"granularity": "minute",
			},
			expectedHours: 6,
			expectedGran:  usageStats.ByMinute,
			description:   "Should calculate correct hours and use specified granularity",
			timeRelative:  true,
		},
		{
			name: "Valid timestamps with auto granularity",
			jsonInput: map[string]interface{}{
				"startEpoch": time.Now().Add(-36 * time.Hour).Unix(),
				"endEpoch":   time.Now().Unix(),
			},
			expectedHours: 36,
			expectedGran:  usageStats.Hourly,
			description:   "Should automatically determine hourly granularity for 36 hours",
			timeRelative:  true,
		},
		{
			name: "Valid timestamps with numeric granularity",
			jsonInput: map[string]interface{}{
				"startEpoch":  time.Now().Add(-2 * time.Hour).Unix(),
				"endEpoch":    time.Now().Unix(),
				"granularity": json.Number("1"),
			},
			expectedHours: 2,
			expectedGran:  usageStats.Hourly,
			description:   "Should handle numeric granularity value",
			timeRelative:  true,
		},
		{
			name: "Invalid start timestamp",
			jsonInput: map[string]interface{}{
				"startEpoch": "invalid",
				"endEpoch":   time.Now().Unix(),
			},
			expectedHours: defaultPastHours,
			expectedGran:  usageStats.Daily,
			description:   "Should return defaults for invalid start timestamp",
			timeRelative:  true,
		},
		{
			name: "Start timestamp after end timestamp",
			jsonInput: map[string]interface{}{
				"startEpoch": time.Now().Unix(),
				"endEpoch":   time.Now().Add(-24 * time.Hour).Unix(),
			},
			expectedHours: defaultPastHours,
			expectedGran:  usageStats.Daily,
			description:   "Should return defaults when start is after end",
			timeRelative:  true,
		},
		{
			name: "String 'now' for end timestamp",
			jsonInput: map[string]interface{}{
				"startEpoch": time.Now().Add(-48 * time.Hour).Unix(),
				"endEpoch":   "now",
			},
			expectedHours: 48,
			expectedGran:  usageStats.Hourly,
			description:   "Should handle 'now' string for end timestamp",
			timeRelative:  true,
		},
		{
			name: "Relative time 'now-24h'",
			jsonInput: map[string]interface{}{
				"startEpoch": "now-24h",
				"endEpoch":   "now",
			},
			expectedHours: 24,
			expectedGran:  usageStats.Hourly,
			description:   "Should handle relative time expressions",
			timeRelative:  true,
		},
		{
			name: "Millisecond timestamps",
			jsonInput: map[string]interface{}{
				"startEpoch": time.Now().Add(-72 * time.Hour).UnixMilli(),
				"endEpoch":   time.Now().UnixMilli(),
			},
			expectedHours: 72,
			expectedGran:  usageStats.Daily,
			description:   "Should normalize millisecond timestamps to seconds",
			timeRelative:  true,
		},
		{
			name: "Monthly granularity",
			jsonInput: map[string]interface{}{
				"startEpoch":  time.Now().Add(-720 * time.Hour).Unix(), // 30 days
				"endEpoch":    time.Now().Unix(),
				"granularity": "monthly",
			},
			expectedHours: 720,
			expectedGran:  usageStats.Monthly,
			description:   "Should handle monthly granularity",
			timeRelative:  true,
		},
		{
			name: "Float granularity value",
			jsonInput: map[string]interface{}{
				"startEpoch":  time.Now().Add(-10 * time.Hour).Unix(),
				"endEpoch":    time.Now().Unix(),
				"granularity": float64(2),
			},
			expectedHours: 10,
			expectedGran:  usageStats.Daily,
			description:   "Should convert float granularity to appropriate enum",
			timeRelative:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hours, granularity := parseIngestionStatsRequest(tt.jsonInput)
			if tt.timeRelative {
				assert.Equal(t, tt.expectedGran, granularity, tt.description)
				assert.InDelta(t, float64(tt.expectedHours), float64(hours), 2, tt.description)
			} else {
				assert.Equal(t, tt.expectedHours, hours, tt.description)
				assert.Equal(t, tt.expectedGran, granularity, tt.description)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name         string
		input        interface{}
		expected     int64
		description  string
		timeRelative bool
	}{
		{
			name:         "Integer timestamp",
			input:        int64(1617211200),
			expected:     1617211200,
			description:  "Should return the same timestamp",
			timeRelative: false,
		},
		{
			name:         "Millisecond timestamp",
			input:        int64(1617211200000),
			expected:     1617211200,
			description:  "Should convert milliseconds to seconds",
			timeRelative: false,
		},
		{
			name:         "Float timestamp",
			input:        float64(1617211200),
			expected:     1617211200,
			description:  "Should handle float timestamps",
			timeRelative: false,
		},
		{
			name:         "String timestamp",
			input:        "1617211200",
			expected:     1617211200,
			description:  "Should parse string timestamps",
			timeRelative: false,
		},
		{
			name:         "Now string",
			input:        "now",
			expected:     now,
			description:  "Should use current time for 'now'",
			timeRelative: true,
		},
		{
			name:         "Relative time now-1h",
			input:        "now-1h",
			expected:     now - 3600,
			description:  "Should subtract 1 hour from current time",
			timeRelative: true,
		},
		{
			name:         "Relative time now-1d",
			input:        "now-1d",
			expected:     now - 86400,
			description:  "Should subtract 1 day from current time",
			timeRelative: true,
		},
		{
			name:         "Invalid string",
			input:        "invalid",
			expected:     -1,
			description:  "Should return -1 for invalid timestamp",
			timeRelative: false,
		},
		{
			name:         "JSON number",
			input:        json.Number("1617211200"),
			expected:     1617211200,
			description:  "Should handle json.Number type",
			timeRelative: false,
		},
		{
			name:         "Nil value",
			input:        nil,
			expected:     -1,
			description:  "Should return -1 for nil input",
			timeRelative: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseTimestamp(tt.input)
			if tt.timeRelative {
				assert.InDelta(t, float64(tt.expected), float64(result), 2, tt.description)
			} else {
				assert.Equal(t, tt.expected, result, tt.description)
			}
		})
	}
}

func TestParseGranularity(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expected    usageStats.UsageStatsGranularity
		description string
	}{
		{
			name:        "String minute",
			input:       "minute",
			expected:    usageStats.ByMinute,
			description: "Should parse 'minute' string",
		},
		{
			name:        "String hourly",
			input:       "hourly",
			expected:    usageStats.Hourly,
			description: "Should parse 'hourly' string",
		},
		{
			name:        "String daily",
			input:       "daily",
			expected:    usageStats.Daily,
			description: "Should parse 'daily' string",
		},
		{
			name:        "String monthly",
			input:       "monthly",
			expected:    usageStats.Monthly,
			description: "Should parse 'monthly' string",
		},
		{
			name:        "Integer 1",
			input:       1,
			expected:    usageStats.Hourly,
			description: "Should convert integer 1 to Hourly",
		},
		{
			name:        "Integer 2",
			input:       2,
			expected:    usageStats.Daily,
			description: "Should convert integer 2 to Daily",
		},
		{
			name:        "Integer 3",
			input:       3,
			expected:    usageStats.ByMinute,
			description: "Should convert integer 3 to ByMinute",
		},
		{
			name:        "Integer 4",
			input:       4,
			expected:    usageStats.Monthly,
			description: "Should convert integer 3 to Monthly",
		},
		{
			name:        "Float 1.0",
			input:       float64(1.0),
			expected:    usageStats.Hourly,
			description: "Should convert float 1.0 to Hourly",
		},
		{
			name:        "JSON Number 2",
			input:       json.Number("2"),
			expected:    usageStats.Daily,
			description: "Should convert json.Number to Daily",
		},
		{
			name:        "Invalid string",
			input:       "invalid",
			expected:    usageStats.Daily,
			description: "Should default to Daily for invalid string",
		},
		{
			name:        "Unknown type",
			input:       []string{"test"},
			expected:    usageStats.Daily,
			description: "Should default to Daily for unknown type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseGranularity(tt.input)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestDetermineGranularity(t *testing.T) {
	tests := []struct {
		name        string
		hours       uint64
		expected    usageStats.UsageStatsGranularity
		description string
	}{
		{
			name:        "Less than 24 hours",
			hours:       20,
			expected:    usageStats.ByMinute,
			description: "Should use ByMinute for < 24 hours",
		},
		{
			name:        "Exactly 24 hours",
			hours:       24,
			expected:    usageStats.Hourly,
			description: "Should use Hourly for 24 hours",
		},
		{
			name:        "Between 24 and 48 hours",
			hours:       36,
			expected:    usageStats.Hourly,
			description: "Should use Hourly for 24-48 hours",
		},
		{
			name:        "Exactly 48 hours",
			hours:       48,
			expected:    usageStats.Hourly,
			description: "Should use Hourly for exactly 48 hours",
		},
		{
			name:        "More than 48 hours",
			hours:       72,
			expected:    usageStats.Daily,
			description: "Should use Daily for > 48 hours",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineGranularity(tt.hours)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestParseAlphaNumTime(t *testing.T) {
	defaultValue := uint64(7 * 24)

	tests := []struct {
		name          string
		input         string
		defaultVal    uint64
		expectedHours uint64
		expectedGran  usageStats.UsageStatsGranularity
		description   string
	}{
		{
			name:          "Valid hours format",
			input:         "now-5h",
			defaultVal:    defaultValue,
			expectedHours: 5,
			expectedGran:  usageStats.ByMinute,
			description:   "Should parse 5 hours correctly",
		},
		{
			name:          "Valid days format",
			input:         "now-3d",
			defaultVal:    defaultValue,
			expectedHours: 72,
			expectedGran:  usageStats.Daily,
			description:   "Should parse 3 days (72 hours) correctly",
		},
		{
			name:          "Invalid format",
			input:         "now-5x",
			defaultVal:    defaultValue,
			expectedHours: defaultValue,
			expectedGran:  usageStats.Daily,
			description:   "Should return default for invalid unit",
		},
		{
			name:          "Too short input",
			input:         "now-",
			defaultVal:    defaultValue,
			expectedHours: defaultValue,
			expectedGran:  usageStats.Daily,
			description:   "Should return default for input that's too short",
		},
		{
			name:          "With spaces",
			input:         "now - 12h",
			defaultVal:    defaultValue,
			expectedHours: 12,
			expectedGran:  usageStats.ByMinute,
			description:   "Should handle spaces in input",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hours, granularity := parseAlphaNumTime(tt.input, tt.defaultVal)
			assert.Equal(t, tt.expectedHours, hours, tt.description)
			assert.Equal(t, tt.expectedGran, granularity, tt.description)
		})
	}
}
