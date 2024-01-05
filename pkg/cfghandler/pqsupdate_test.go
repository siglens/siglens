package cfghandler

import (
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestSavePQSConfigToRunMod(t *testing.T) {
	testCases := []struct {
		pqsEnabled       string
		expectConfigFile bool
	}{
		{"enabled", true},
		{"disabled", true},
	}
	filepath := "/Users/davleen/Downloads/hyperion_new/oss-siglens/siglens/data/common/runmod.cfg"
	for _, tc := range testCases {
		t.Run(tc.pqsEnabled, func(t *testing.T) {
			err := SavePQSConfigToRunMod(filepath, tc.pqsEnabled)
			assert.NoError(t, err, "Error in SavePQSConfigToRunMod")
		})
	}
}
func TestExtractReadRunModConfig(t *testing.T) {
	cases := []struct {
		name     string
		input    []byte
		expected config.RunModConfig
		wantErr  bool
	}{
		{
			name:  "Valid Enabled Config",
			input: []byte(`{"pqsEnabled": "enabled"}`),
			expected: config.RunModConfig{
				PQSEnabled: "enabled",
			},
			wantErr: false,
		},
		{
			name:  "Valid Disabled Config",
			input: []byte(`{"pqsEnabled": "disabled"}`),
			expected: config.RunModConfig{
				PQSEnabled: "disabled",
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actualConfig, err := config.ExtractReadRunModConfig(tc.input)

			if tc.wantErr {
				assert.Error(t, err, "Expected an error in %s", tc.name)
			} else {
				assert.NoError(t, err, "Unexpected error in %s: %v", tc.name, err)
				assert.Equal(t, tc.expected, actualConfig, "Mismatch in config for %s", tc.name)
			}
		})
	}
}
