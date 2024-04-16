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

	"github.com/siglens/siglens/pkg/config/common"
	"github.com/stretchr/testify/assert"
)

func Test_ExtractConfigData(t *testing.T) {
	flag.Parse()
	cases := []struct {
		input    []byte
		expected common.Configuration
	}{
		{ // case 1 - For correct input parameters and values
			[]byte(`
 ingestListenIP: "0.0.0.0"
 queryListenIP: "0.0.0.0"
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
 s3IngestQueueName: ""
 s3IngestQueueRegion: ""
 s3IngestBufferSize: 1000
 maxParallelS3IngestBuffers: 10
 queryHostname: "abc:123"
 PQSEnabled: bad string
 analyticsEnabled: false
 agileAggsEnabled: false
 safeMode: true
 tracing:
   endpoint: "http://localhost:4317"
   serviceName: "siglens"
   samplingPercentage: 100
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
 `),

			common.Configuration{
				IngestListenIP:             "0.0.0.0",
				QueryListenIP:              "0.0.0.0",
				IngestPort:                 9090,
				IngestUrl:                  "http://localhost:9090",
				QueryPort:                  5122,
				EventTypeKeywords:          []string{"utm_content"},
				QueryNode:                  "true",
				IngestNode:                 "true",
				SegFlushIntervalSecs:       5,
				DataPath:                   "data/",
				S3:                         common.S3Config{Enabled: true, BucketName: "test-1", BucketPrefix: "", RegionName: "us-east-1"},
				RetentionHours:             90,
				TimeStampKey:               "timestamp",
				MaxSegFileSize:             10,
				LicenseKeyPath:             "./",
				ESVersion:                  "6.8.20",
				DataDiskThresholdPercent:   85,
				MemoryThresholdPercent:     80,
				S3IngestQueueName:          "",
				S3IngestQueueRegion:        "",
				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "abc:123",
				PQSEnabled:                 "false",
				PQSEnabledConverted:        false,
				AnalyticsEnabled:           "false",
				AnalyticsEnabledConverted:  false,
				AgileAggsEnabled:           "false",
				AgileAggsEnabledConverted:  false,
				SafeServerStart:            true,
				Log:                        common.LogConfig{LogPrefix: "./pkg/ingestor/httpserver/", LogFileRotationSizeMB: 100, CompressLogFile: false},
				Tracing:                    common.TracingConfig{Endpoint: "http://localhost:4317", ServiceName: "siglens", SamplingPercentage: 100},
			},
		},
		{ // case 2 - For wrong input type, show error message
			[]byte(`
 ingestListenIP: "0.0.0.0"
 queryListenIP: "0.0.0.0"
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
 S3IngestQueueName: ""
 S3IngestQueueRegion: ""
 S3IngestBufferSize: 1000
 MaxParallelS3IngestBuffers: 10
 PQSEnabled: F
 analyticsEnabled: bad string
 AgileAggsEnabled: bad string
 tracing:
   endpoint: ""
   serviceName: ""
   smaplingPercentage: bad string
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
   logFileRotationSizeMB: 1000
   compressLogFile: true
 `),

			common.Configuration{
				IngestListenIP:             "0.0.0.0",
				QueryListenIP:              "0.0.0.0",
				IngestPort:                 9090,
				QueryPort:                  9000,
				IngestUrl:                  "http://localhost:9090",
				EventTypeKeywords:          []string{"utm_content"},
				QueryNode:                  "true",
				IngestNode:                 "true",
				SegFlushIntervalSecs:       1200,
				DataPath:                   "data/",
				S3:                         common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
				RetentionHours:             123,
				TimeStampKey:               "timestamp",
				MaxSegFileSize:             12345,
				LicenseKeyPath:             "./",
				ESVersion:                  "6.8.20",
				DataDiskThresholdPercent:   85,
				MemoryThresholdPercent:     80,
				S3IngestQueueName:          "",
				S3IngestQueueRegion:        "",
				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "",
				PQSEnabled:                 "false",
				PQSEnabledConverted:        false,
				AnalyticsEnabled:           "true",
				AnalyticsEnabledConverted:  true,
				AgileAggsEnabled:           "true",
				AgileAggsEnabledConverted:  true,
				SafeServerStart:            false,
				Log:                        common.LogConfig{LogPrefix: "./pkg/ingestor/httpserver/", LogFileRotationSizeMB: 1000, CompressLogFile: true},
				Tracing:                    common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 0},
			},
		},
		{ // case 3 - Error out on bad yaml
			[]byte(`
invalid input, we should error out
`),

			common.Configuration{
				IngestListenIP:           "0.0.0.0",
				QueryListenIP:            "0.0.0.0",
				IngestPort:               8081,
				QueryPort:                0,
				IngestUrl:                "http://localhost:8081",
				EventTypeKeywords:        []string{"eventType"},
				QueryNode:                "true",
				IngestNode:               "true",
				SegFlushIntervalSecs:     30,
				DataPath:                 "data/",
				S3:                       common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
				RetentionHours:           90,
				TimeStampKey:             "timestamp",
				MaxSegFileSize:           1_073_741_824,
				LicenseKeyPath:           "./",
				ESVersion:                "6.8.20",
				DataDiskThresholdPercent: 85,
				MemoryThresholdPercent:   80,
				S3IngestQueueName:        "",
				S3IngestQueueRegion:      "",

				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "",
				AnalyticsEnabled:           "true",
				AnalyticsEnabledConverted:  true,
				AgileAggsEnabled:           "true",
				AgileAggsEnabledConverted:  true,
				Log:                        common.LogConfig{LogPrefix: "", LogFileRotationSizeMB: 100, CompressLogFile: false},
				Tracing:                    common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 1},
			},
		},
		{ // case 4 - For no input, pick defaults
			[]byte(`
a: b
`),
			common.Configuration{
				IngestListenIP:             "0.0.0.0",
				QueryListenIP:              "0.0.0.0",
				IngestPort:                 8081,
				QueryPort:                  5122,
				IngestUrl:                  "http://localhost:8081",
				EventTypeKeywords:          []string{"eventType"},
				QueryNode:                  "true",
				IngestNode:                 "true",
				SegFlushIntervalSecs:       5,
				DataPath:                   "data/",
				S3:                         common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
				RetentionHours:             90 * 24,
				TimeStampKey:               "timestamp",
				MaxSegFileSize:             1_073_741_824,
				LicenseKeyPath:             "./",
				ESVersion:                  "6.8.20",
				DataDiskThresholdPercent:   85,
				MemoryThresholdPercent:     80,
				S3IngestQueueName:          "",
				S3IngestQueueRegion:        "",
				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "",
				PQSEnabled:                 "false",
				PQSEnabledConverted:        false,
				SafeServerStart:            false,
				AnalyticsEnabled:           "true",
				AnalyticsEnabledConverted:  true,
				AgileAggsEnabled:           "true",
				AgileAggsEnabledConverted:  true,
				Log:                        common.LogConfig{LogPrefix: "", LogFileRotationSizeMB: 100, CompressLogFile: false},
				Tracing:                    common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 0},
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
