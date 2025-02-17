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
	"bufio"
	"fmt"
	"io"
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

const pathToSiglens = "../../" // Relative to this package.

type siglensServer struct {
	dir       string // Where to store info for this server (e.g., config file, data dir)
	config    *common.Configuration
	pid       int // Process ID
	isRunning bool
}

func newSiglensServer(dir string, config *common.Configuration) *siglensServer {
	return &siglensServer{
		dir:    dir,
		config: config,
	}
}

func (s *siglensServer) Start(t *testing.T) {
	t.Helper()

	yamlBytes, err := yaml.Marshal(s.config)
	require.NoError(t, err)

	configPath := filepath.Join(s.dir, "config.yaml")
	err = os.WriteFile(configPath, yamlBytes, 0644)
	require.NoError(t, err)

	// Start the server.
	cmd := exec.Command("go", "run", "cmd/siglens/main.go", "--config", configPath)
	cmd.Dir = pathToSiglens
	err = cmd.Start()
	require.NoError(t, err)

	s.pid = cmd.Process.Pid
	s.isRunning = true
}

func (s *siglensServer) StopGracefully(t *testing.T) {
	t.Helper()

	s.verifyCanBeStopped(t)
	terminal(t, fmt.Sprintf("kill -s SIGTERM %d", s.pid))

	s.pid = 0
	s.isRunning = false
}

func (s *siglensServer) ForceStop(t *testing.T) {
	t.Helper()

	s.verifyCanBeStopped(t)
	terminal(t, fmt.Sprintf("kill -s SIGKILL %d", s.pid))

	s.pid = 0
	s.isRunning = false
}

func (s *siglensServer) verifyCanBeStopped(t *testing.T) {
	t.Helper()

	if !s.isRunning {
		t.Logf("Server is not running")
		t.FailNow()
	}

	if s.pid == 0 {
		t.Logf("Server PID is 0")
		t.FailNow()
	}
}

func terminal(t *testing.T, command string) string {
	t.Helper()
	t.Logf("$ %s\n", command)

	cmd := exec.Command("bash", "-c", command)
	output, err := cmd.CombinedOutput()
	if len(output) > 0 {
		t.Log(string(output))
	}
	require.NoError(t, err)

	return string(output)
}

func sigclient(t *testing.T, command string) {
	t.Helper()
	t.Logf("sigclient $ %s\n", command)

	parts := strings.Split(command, " ")
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = filepath.Join(pathToSiglens, "tools/sigclient")

	// Obtain pipes for the command's stdout and stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("Failed to get stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("Failed to get stderr pipe: %v", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start command: %v", err)
	}

	// Channel to signal completion
	done := make(chan struct{})

	// Function to read and log output
	readAndLog := func(pipe io.ReadCloser, prefix string) {
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			t.Logf("%s: %s", prefix, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			t.Errorf("Error reading %s: %v", prefix, err)
		}
		done <- struct{}{}
	}

	// Read stdout and stderr concurrently
	go readAndLog(stdoutPipe, "stdout")
	go readAndLog(stderrPipe, "stderr")

	// Wait for both goroutines to finish
	<-done
	<-done

	// Wait for the command to complete
	if err := cmd.Wait(); err != nil {
		t.Fatalf("Command execution failed: %v", err)
	}
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
