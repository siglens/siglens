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

package fileutils

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
)

func TestAddAccessLogEntry(t *testing.T) {
	// Create a temporary test access.log file
	tempLogFile, err := os.CreateTemp("", "test_access.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempLogFile.Name())

	// Example data
	data := dtypeutils.LogFileData{
		TimeStamp:   time.Now().Format(time.RFC3339),
		UserName:    "test_user",
		QueryID:     0,
		URI:         "/example",
		RequestBody: "test_body",
		StatusCode:  200,
		Duration:    int64(time.Second * 2),
	}

	// Call the function with the temporary logFile
	allowWebsocket := false
	AddLogEntry(data, allowWebsocket, tempLogFile)

	// Read the content of the temporary file
	content, err := os.ReadFile(tempLogFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Validate the content of the file
	expectedLogEntry := fmt.Sprintf("%s %s %d %s %s %d %d\n",
		data.TimeStamp,
		data.UserName,
		data.QueryID,
		data.URI,
		data.RequestBody,
		data.StatusCode,
		int(data.Duration))

	if !strings.Contains(string(content), expectedLogEntry) {
		t.Errorf("Expected log entry not found in the file. Expected:\n%s\nGot:\n%s", expectedLogEntry, string(content))
	}
}

func Test_AddLogEntryValidations(t *testing.T) {
	cases := []struct {
		input dtypeutils.LogFileData
	}{
		{ // case#1
			dtypeutils.LogFileData{
				TimeStamp:   "",
				UserName:    "",
				URI:         "http:///",
				RequestBody: "{\n  \"indexName\":\"traces\"\n}",
				StatusCode:  0,
				Duration:    0,
			},
		},
		{ //case 2
			dtypeutils.LogFileData{
				StatusCode: 101,
			},
		},
	}

	for _, test := range cases {
		// Create a temporary test access.log file
		tempLogFile, err := os.CreateTemp("", "test_access.log")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempLogFile.Name())

		// Call the function with the temporary logFile
		allowWebsocket := false
		fileName := tempLogFile.Name()
		AddLogEntry(test.input, allowWebsocket, tempLogFile)

		// Read the content of the temporary file
		content, err := os.ReadFile(fileName)
		if err != nil {
			t.Fatal(err)
		}

		if len(content) != 0 {
			t.Errorf("Expected log entry not found in the file. Expected:\n%s\nGot:\n%s", "", string(content))
		}

	}

}
