package cfghandler

import (
	"testing"

	"fmt"
	"os"

	"github.com/siglens/siglens/pkg/config"
	commonconfig "github.com/siglens/siglens/pkg/config/common"
	"github.com/stretchr/testify/assert"
)

func TestSavePQSConfigToRunMod(t *testing.T) {
	testCases := []struct {
		pqsEnabled       bool
		expectConfigFile bool
	}{
		{true, true},
		{false, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("pqsEnabled_%t", tc.pqsEnabled), func(t *testing.T) {
			tempFile, err := os.CreateTemp("", "runmodcfg_*.json")
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
		expected commonconfig.RunModConfig
		wantErr  bool
	}{
		{
			name:  "Valid Enabled Config",
			input: []byte(`{"pqsEnabled": true}`),
			expected: commonconfig.RunModConfig{
				PQSEnabled: true,
			},
			wantErr: false,
		},
		{
			name:  "Valid Disabled Config",
			input: []byte(`{"pqsEnabled": false}`),
			expected: commonconfig.RunModConfig{
				PQSEnabled: false,
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
