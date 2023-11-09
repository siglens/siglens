/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"flag"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ExtractConfigData(t *testing.T) {
	flag.Parse()
	cases := []struct {
		input    []byte
		expected Configuration
	}{
		{ // case 1 - For correct input parameters and values
			[]byte(`
 ingestPort: 9090
 eventTypeKeywords: ["utm_content"]
 baseLogDir: "./pkg/ingestor/httpserver/"
 queryNode: true
 ingestNode: true
 seedNode: true
 segreaderNode: true
 metareaderNode: true
 DataPath: "data/"
 s3:
  enabled: true
  bucketName: "test-1"
  bucketPrefix: ""
  regionName: "us-east-1"
 retentionHours: 90
 TimeStampKey: "timestamp"
 maxSegFileSize: 10
 licensekeyPath: "./"
 esVersion: "6.8.20"
 maxVirtualTables: 10_000
 logFileRotationSizeMB: 100
 compressLogFile: false
 dataDiskThresholdPercent: 85
 memoryThresholdPercent: 80
 partitionCountConsistentHasher: 271
 replicationFactorConsistentHasher: 40
 loadConsistentHasher: 1.2
 grpcPort: 0
 s3IngestQueueName: ""
 s3IngestQueueRegion: ""
 s3IngestBufferSize: 1000
 maxParallelS3IngestBuffers: 10
 queryHostname: "abc:123"
 pqsEnabled: bad string
 analyticsEnabled: false
 safeMode: true
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
 `),

			Configuration{
				IngestPort:                 9090,
				IngestUrl:                  "http://localhost:9090",
				QueryPort:                  80,
				EventTypeKeywords:          []string{"utm_content"},
				QueryNode:                  "true",
				IngestNode:                 "true",
				SegFlushIntervalSecs:       5,
				DataPath:                   "data/",
				S3:                         S3Config{true, "test-1", "", "us-east-1"},
				RetentionHours:             90,
				TimeStampKey:               "timestamp",
				MaxSegFileSize:             10,
				LicenseKeyPath:             "./",
				ESVersion:                  "6.8.20",
				DataDiskThresholdPercent:   85,
				MemoryThresholdPercent:     80,
				GRPCPort:                   50051,
				S3IngestQueueName:          "",
				S3IngestQueueRegion:        "",
				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "abc:123",
				PQSEnabled:                 "true",
				pqsEnabledConverted:        true,
				AnalyticsEnabled:           "false",
				analyticsEnabledConverted:  false,
				SafeServerStart:            true,
				Log:                        LogConfig{"./pkg/ingestor/httpserver/", 100, false},
			},
		},
		{ // case 2 - For wrong input type, show error message
			[]byte(`
 ingestPort: 9090
 queryPort: 9000
 eventTypeKeywords: ["utm_content"]
 queryNode: true
 ingestNode: true
 seedNode: true
 segreaderNode: true
 metareaderNode: true
 segFlushIntervalSecs: 1200
 DataPath: "data/"
 retentionHours: 123
 TimeStampKey: "timestamp"
 maxSegFileSize: 12345
 licensekeyPath: "./"
 esVersion: "6.8.20"
 dataDiskThresholdPercent: 85
 memoryThresholdPercent: 80
 partitionCountConsistentHasher: 271
 replicationFactorConsistentHasher: 40
 loadConsistentHasher: 1.2
 grpcPort: 66
 S3IngestQueueName: ""
 S3IngestQueueRegion: ""
 S3IngestBufferSize: 1000
 MaxParallelS3IngestBuffers: 10
 pqsEnabled: F
 analyticsEnabled: bad string
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
   logFileRotationSizeMB: 1000
   compressLogFile: true
 `),

			Configuration{
				IngestPort:                 9090,
				QueryPort:                  9000,
				IngestUrl:                  "http://localhost:9090",
				EventTypeKeywords:          []string{"utm_content"},
				QueryNode:                  "true",
				IngestNode:                 "true",
				SegFlushIntervalSecs:       1200,
				DataPath:                   "data/",
				S3:                         S3Config{false, "", "", ""},
				RetentionHours:             123,
				TimeStampKey:               "timestamp",
				MaxSegFileSize:             12345,
				LicenseKeyPath:             "./",
				ESVersion:                  "6.8.20",
				DataDiskThresholdPercent:   85,
				MemoryThresholdPercent:     80,
				GRPCPort:                   66,
				S3IngestQueueName:          "",
				S3IngestQueueRegion:        "",
				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "",
				PQSEnabled:                 "F",
				pqsEnabledConverted:        false,
				AnalyticsEnabled:           "true",
				analyticsEnabledConverted:  true,
				SafeServerStart:            false,
				Log:                        LogConfig{"./pkg/ingestor/httpserver/", 1000, true},
			},
		},
		{ // case 3 - Error out on bad yaml
			[]byte(`
invalid input, we should error out
`),

			Configuration{
				IngestPort:               8081,
				QueryPort:                0,
				IngestUrl:                "http://localhost:8081",
				EventTypeKeywords:        []string{"eventType"},
				QueryNode:                "true",
				IngestNode:               "true",
				SegFlushIntervalSecs:     30,
				DataPath:                 "data/",
				S3:                       S3Config{false, "", "", ""},
				RetentionHours:           90,
				TimeStampKey:             "timestamp",
				MaxSegFileSize:           1_073_741_824,
				LicenseKeyPath:           "./",
				ESVersion:                "6.8.20",
				DataDiskThresholdPercent: 85,
				MemoryThresholdPercent:   80,
				GRPCPort:                 50051,
				S3IngestQueueName:        "",
				S3IngestQueueRegion:      "",

				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "",
				AnalyticsEnabled:           "true",
				analyticsEnabledConverted:  true,
				Log:                        LogConfig{"", 100, false},
			},
		},
		{ // case 4 - For no input, pick defaults
			[]byte(`
a: b
`),
			Configuration{
				IngestPort:                 8081,
				QueryPort:                  80,
				IngestUrl:                  "http://localhost:8081",
				EventTypeKeywords:          []string{"eventType"},
				QueryNode:                  "true",
				IngestNode:                 "true",
				SegFlushIntervalSecs:       5,
				DataPath:                   "data/",
				S3:                         S3Config{false, "", "", ""},
				RetentionHours:             90 * 24,
				TimeStampKey:               "timestamp",
				MaxSegFileSize:             1_073_741_824,
				LicenseKeyPath:             "./",
				ESVersion:                  "6.8.20",
				DataDiskThresholdPercent:   85,
				MemoryThresholdPercent:     80,
				GRPCPort:                   50051,
				S3IngestQueueName:          "",
				S3IngestQueueRegion:        "",
				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "",
				PQSEnabled:                 "true",
				pqsEnabledConverted:        true,
				SafeServerStart:            false,
				AnalyticsEnabled:           "true",
				analyticsEnabledConverted:  true,
				Log:                        LogConfig{"", 100, false},
			},
		},
	}
	for i, test := range cases {
		actualConfig, err := ExtractConfigData(test.input)
		if i == 2 {
			assert.Error(t, err)
			continue
		}

		assert.NoError(t, err, fmt.Sprintf("Comparison failed, test=%v", i+1))
		assert.EqualValues(t, test.expected, actualConfig, fmt.Sprintf("Comparison failed, test=%v", i+1))
	}
}
