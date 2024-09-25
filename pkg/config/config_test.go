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

package config

import (
	"flag"
	"fmt"
	"testing"

	"github.com/pbnjay/memory"
	"github.com/siglens/siglens/pkg/config/common"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_ExtractConfigData(t *testing.T) {
	flag.Parse()
	cases := []struct {
		input       []byte
		shouldError bool
		expected    common.Configuration
	}{
		{ // case 1 - For correct input parameters and values
			[]byte(`
 ingestListenIP: "0.0.0.0"
 queryListenIP: "0.0.0.0"
 ingestPort: 9090
 eventTypeKeywords: ["utm_content"]
 queryNode: true
 ingestNode: true
 dataPath: "data/"
 s3:
  enabled: true
  bucketName: "test-1"
  bucketPrefix: ""
  regionName: "us-east-1"
 retentionHours: 90
 timestampKey: "timestamp"
 maxSegFileSize: 10
 licenseKeyPath: "./"
 esVersion: "6.8.20"
 dataDiskThresholdPercent: 85
 memoryThresholdPercent: 80
 s3IngestQueueName: ""
 s3IngestQueueRegion: ""
 s3IngestBufferSize: 1000
 maxParallelS3IngestBuffers: 10
 queryHostname: "abc:123"
 pqsEnabled: bad string
 analyticsEnabled: false
 agileAggsEnabled: false
 safeMode: true
 tracing:
   endpoint: "http://localhost:4317"
   serviceName: "siglens"
   samplingPercentage: 100
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
   logFileRotationSizeMB: 100
   compressLogFile: false
 `),
			false,
			common.Configuration{
				IngestListenIP:              "0.0.0.0",
				QueryListenIP:               "0.0.0.0",
				IngestPort:                  9090,
				IngestUrl:                   "http://localhost:9090",
				QueryPort:                   5122,
				EventTypeKeywords:           []string{"utm_content"},
				QueryNode:                   "true",
				IngestNode:                  "true",
				IdleWipFlushIntervalSecs:    5,
				MaxWaitWipFlushIntervalSecs: 30,
				DataPath:                    "data/",
				S3:                          common.S3Config{Enabled: true, BucketName: "test-1", BucketPrefix: "", RegionName: "us-east-1"},
				RetentionHours:              90,
				TimeStampKey:                "timestamp",
				MaxSegFileSize:              10,
				LicenseKeyPath:              "./",
				ESVersion:                   "6.8.20",
				DataDiskThresholdPercent:    85,
				MemoryThresholdPercent:      80,
				S3IngestQueueName:           "",
				S3IngestQueueRegion:         "",
				S3IngestBufferSize:          1000,
				MaxParallelS3IngestBuffers:  10,
				QueryHostname:               "abc:123",
				PQSEnabled:                  "true",
				PQSEnabledConverted:         true,
				AnalyticsEnabled:            "false",
				AnalyticsEnabledConverted:   false,
				AgileAggsEnabled:            "false",
				AgileAggsEnabledConverted:   false,
				DualCaseCheck:               "true",
				DualCaseCheckConverted:      true,
				SafeServerStart:             true,
				Log:                         common.LogConfig{LogPrefix: "./pkg/ingestor/httpserver/", LogFileRotationSizeMB: 100, CompressLogFile: false},
				Tracing:                     common.TracingConfig{Endpoint: "http://localhost:4317", ServiceName: "siglens", SamplingPercentage: 100},
			},
		},
		{ // case 2 - For wrong input type, show error message
			[]byte(`
 memoryThresholdPercent: not a number
 `),
			true,
			common.Configuration{},
		},
		{ // case 3 - Error out on bad yaml
			[]byte(`
invalid input, we should error out
`),
			true,
			common.Configuration{},
		},
		{ // case 4 - Error out on invalid fields
			[]byte(`
invalidField: "invalid"
`),
			true,
			common.Configuration{},
		},
		{ // case 5 - For no input, pick defaults
			[]byte(``),
			false,
			common.Configuration{
				IngestListenIP:              "0.0.0.0",
				QueryListenIP:               "0.0.0.0",
				IngestPort:                  8081,
				QueryPort:                   5122,
				IngestUrl:                   "http://localhost:8081",
				EventTypeKeywords:           []string{"eventType"},
				QueryNode:                   "true",
				IngestNode:                  "true",
				IdleWipFlushIntervalSecs:    5,
				MaxWaitWipFlushIntervalSecs: 30,
				DataPath:                    "data/",
				S3:                          common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
				RetentionHours:              15 * 24,
				TimeStampKey:                "timestamp",
				MaxSegFileSize:              4_294_967_296,
				LicenseKeyPath:              "./",
				ESVersion:                   "6.8.20",
				DataDiskThresholdPercent:    85,
				MemoryThresholdPercent:      80,
				S3IngestQueueName:           "",
				S3IngestQueueRegion:         "",
				S3IngestBufferSize:          1000,
				MaxParallelS3IngestBuffers:  10,
				QueryHostname:               "localhost:5122",
				PQSEnabled:                  "true",
				PQSEnabledConverted:         true,
				SafeServerStart:             false,
				AnalyticsEnabled:            "true",
				AnalyticsEnabledConverted:   true,
				AgileAggsEnabled:            "true",
				AgileAggsEnabledConverted:   true,
				DualCaseCheck:               "true",
				DualCaseCheckConverted:      true,
				Log:                         common.LogConfig{LogPrefix: "", LogFileRotationSizeMB: 100, CompressLogFile: false},
				Tracing:                     common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 0},
			},
		},
	}
	for i, test := range cases {
		actualConfig, err := ExtractConfigData(test.input)
		if test.shouldError {
			assert.Error(t, err, fmt.Sprintf("test=%v should have errored", i+1))
			continue
		} else {
			assert.NoError(t, err, fmt.Sprintf("test=%v should not have errored", i+1))
		}

		if segutils.ConvertUintBytesToMB(memory.TotalMemory()) < SIZE_8GB_IN_MB {
			assert.Equal(t, uint64(50), actualConfig.MemoryThresholdPercent)
			// If memory is less than 8GB, config by default returns 50% as the threshold
			// For testing purpose resetting it to 80%
			actualConfig.MemoryThresholdPercent = 80
		}
		assert.NoError(t, err, fmt.Sprintf("Comparison failed, test=%v", i+1))
		assert.EqualValues(t, test.expected, actualConfig, fmt.Sprintf("Comparison failed, test=%v", i+1))
	}
}
