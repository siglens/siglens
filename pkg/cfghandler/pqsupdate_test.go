package cfghandler

import (
	"testing"

	"io/ioutil"
	"os"

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

	for _, tc := range testCases {
		t.Run(tc.pqsEnabled, func(t *testing.T) {
			tempFile, err := ioutil.TempFile("", "runmodcfg_*.json")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			tempFilePath := tempFile.Name()
			defer os.Remove(tempFilePath)

			err = SavePQSConfigToRunMod(tempFilePath, tc.pqsEnabled)
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
