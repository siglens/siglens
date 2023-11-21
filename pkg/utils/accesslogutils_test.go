package utils

import (
	"fmt"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAddAccessLogEntry(t *testing.T) {
	// Create a temporary test access.log file
	tempLogFile, err := ioutil.TempFile("", "test_access.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempLogFile.Name())

	// Example data
	data := dtypeutils.AccessLogData{
		TimeStamp:   time.Now().Format(time.RFC3339),
		UserName:    "test_user",
		URI:         "/example",
		RequestBody: "test_body",
		StatusCode:  200,
		Duration:    int64(time.Second * 2),
	}

	// Call the function with the temporary logFile
	fileName := tempLogFile.Name()
	AddAccessLogEntry(data, fileName)

	// Read the content of the temporary file
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}

	// Validate the content of the file
	expectedLogEntry := fmt.Sprintf("%s %s %s %s %d %d\n",
		data.TimeStamp,
		data.UserName,
		data.URI,
		data.RequestBody,
		data.StatusCode,
		int(data.Duration))

	if !strings.Contains(string(content), expectedLogEntry) {
		t.Errorf("Expected log entry not found in the file. Expected:\n%s\nGot:\n%s", expectedLogEntry, string(content))
	}
}
