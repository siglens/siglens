// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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

package e2etests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/config/common"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func terminal(t *testing.T, command string) {
	t.Helper()
	t.Logf("$ %s\n", command)

	parts := strings.Split(command, " ")
	cmd := exec.Command(parts[0], parts[1:]...)

	output, err := cmd.CombinedOutput()
	if len(output) > 0 {
		t.Log(string(output))
	}
	require.NoError(t, err)
}

func withSiglens(t *testing.T, config *common.Configuration, fn func()) {
	testDir := t.TempDir()
	dataDir := filepath.Join(testDir, "data")
	config.DataPath = dataDir

	configBytes, err := yaml.Marshal(config)
	require.NoError(t, err)

	configFile := filepath.Join(testDir, "server.yaml")
	err = os.WriteFile(configFile, configBytes, 0644)
	require.NoError(t, err)

	cmd := exec.Command("siglens", "--config", configFile)
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start siglens: %v", err)
	}

	defer func() {
		err := cmd.Process.Kill()
		require.NoError(t, err)
	}()

	// TODO: Find a better way to wait for siglens to be ready.
	time.Sleep(2 * time.Second)

	fn()
}
