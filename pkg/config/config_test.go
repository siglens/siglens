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
	"github.com/siglens/siglens/pkg/utils"
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
 pprofEnabled: false
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
 isNewQueryPipelineEnabled: false
 enableSortIndex: true
 queryTimeoutSecs: 600
 safeMode: true
 tracing:
   endpoint: "http://localhost:4317"
   serviceName: "siglens"
   samplingPercentage: 100
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
   logFileRotationSizeMB: 100
   compressLogFile: false
 compressStatic: false
 memoryLimits:
   maxUsagePercent: 80
   lowMemoryMode: true
   searchPercent: 50
   microIndexPercent: 20
   metadataPercent: 20
   metricsPercent: 10
   bytesPerQuery: 100
 maxOpenColumns: 42
 `),
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
				S3IngestQueueName:           "",
				S3IngestQueueRegion:         "",
				S3IngestBufferSize:          1000,
				MaxParallelS3IngestBuffers:  10,
				QueryHostname:               "abc:123",
				PProfEnabled:                "false",
				PProfEnabledConverted:       false,
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
				CompressStatic:              "false",
				CompressStaticConverted:     false,
				Tracing:                     common.TracingConfig{Endpoint: "http://localhost:4317", ServiceName: "siglens", SamplingPercentage: 100},
				UseNewQueryPipeline:         "false",
				UseNewPipelineConverted:     false,
				EnableSortIndex:             utils.DefaultValue(false).Set(true),
				QueryTimeoutSecs:            600,
				MemoryConfig: common.MemoryConfig{
					MaxUsagePercent: 80,
					LowMemoryMode:   utils.DefaultValue(false).Set(true),
					SearchPercent:   50,
					CMIPercent:      20,
					MetadataPercent: 20,
					MetricsPercent:  10,
					BytesPerQuery:   100,
				},
				MaxOpenColumns: 42,
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
 idleWipFlushIntervalSecs: 1200
 maxWaitWipFlushIntervalSecs: 300
 DataPath: "data/"
 retentionHours: 123
 TimeStampKey: "timestamp"
 maxSegFileSize: 12345
 licensekeyPath: "./"
 esVersion: "6.8.20"
 dataDiskThresholdPercent: 85
 partitionCountConsistentHasher: 271
 replicationFactorConsistentHasher: 40
 loadConsistentHasher: 1.2
 S3IngestQueueName: ""
 S3IngestQueueRegion: ""
 S3IngestBufferSize: 1000
 MaxParallelS3IngestBuffers: 10
 pprofEnabled: Fa
 PQSEnabled: F
 dualCaseCheck: true
 analyticsEnabled: bad string
 AgileAggsEnabled: bad string
 queryTimeoutSecs: 0
 tracing:
   endpoint: ""
   serviceName: ""
   smaplingPercentage: bad string
 log:
   logPrefix: "./pkg/ingestor/httpserver/"
   logFileRotationSizeMB: 1000
   compressLogFile: true
 compressStatic: bad string
 enableSortIndex: bad string
 `),

			common.Configuration{
				IngestListenIP:              "0.0.0.0",
				QueryListenIP:               "0.0.0.0",
				IngestPort:                  9090,
				QueryPort:                   9000,
				IngestUrl:                   "http://localhost:9090",
				EventTypeKeywords:           []string{"utm_content"},
				QueryNode:                   "true",
				IngestNode:                  "true",
				IdleWipFlushIntervalSecs:    60,
				MaxWaitWipFlushIntervalSecs: 60,
				DataPath:                    "data/",
				S3:                          common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
				RetentionHours:              123,
				TimeStampKey:                "timestamp",
				MaxSegFileSize:              12345,
				LicenseKeyPath:              "./",
				ESVersion:                   "6.8.20",
				DataDiskThresholdPercent:    85,
				S3IngestQueueName:           "",
				S3IngestQueueRegion:         "",
				S3IngestBufferSize:          1000,
				MaxParallelS3IngestBuffers:  10,
				QueryHostname:               "localhost:9000",
				PProfEnabled:                "true",
				PProfEnabledConverted:       true,
				PQSEnabled:                  "true",
				PQSEnabledConverted:         true,
				DualCaseCheck:               "true",
				DualCaseCheckConverted:      true,
				AnalyticsEnabled:            "true",
				AnalyticsEnabledConverted:   true,
				AgileAggsEnabled:            "true",
				AgileAggsEnabledConverted:   true,
				SafeServerStart:             false,
				Log:                         common.LogConfig{LogPrefix: "./pkg/ingestor/httpserver/", LogFileRotationSizeMB: 1000, CompressLogFile: true},
				CompressStatic:              "true",
				CompressStaticConverted:     true,
				Tracing:                     common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 0},
				UseNewQueryPipeline:         "true",
				UseNewPipelineConverted:     true,
				EnableSortIndex:             utils.DefaultValue(false),
				MemoryConfig: common.MemoryConfig{
					MaxUsagePercent: 80,
					LowMemoryMode:   utils.DefaultValue(false),
					SearchPercent:   DEFAULT_SEG_SEARCH_MEM_PERCENT,
					CMIPercent:      DEFAULT_ROTATED_CMI_MEM_PERCENT,
					MetadataPercent: DEFAULT_METADATA_MEM_PERCENT,
					MetricsPercent:  DEFAULT_METRICS_MEM_PERCENT,
					BytesPerQuery:   DEFAULT_BYTES_PER_QUERY,
				},
				MaxOpenColumns:   DEFAULT_MAX_OPEN_COLUMNS,
				QueryTimeoutSecs: DEFAULT_TIMEOUT_SECONDS,
			},
		},
		{ // case 3 - Error out on bad yaml
			[]byte(`
invalid input, we should error out
`),

			common.Configuration{
				IngestListenIP:              "0.0.0.0",
				QueryListenIP:               "0.0.0.0",
				IngestPort:                  8081,
				QueryPort:                   0,
				IngestUrl:                   "http://localhost:8081",
				EventTypeKeywords:           []string{"eventType"},
				QueryNode:                   "true",
				IngestNode:                  "true",
				IdleWipFlushIntervalSecs:    5,
				MaxWaitWipFlushIntervalSecs: 30,
				DataPath:                    "data/",
				S3:                          common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
				RetentionHours:              90,
				TimeStampKey:                "timestamp",
				MaxSegFileSize:              1_073_741_824,
				LicenseKeyPath:              "./",
				ESVersion:                   "6.8.20",
				DataDiskThresholdPercent:    85,
				S3IngestQueueName:           "",
				S3IngestQueueRegion:         "",

				S3IngestBufferSize:         1000,
				MaxParallelS3IngestBuffers: 10,
				QueryHostname:              "localhost:5122",
				PProfEnabled:               "true",
				PProfEnabledConverted:      true,
				AnalyticsEnabled:           "true",
				AnalyticsEnabledConverted:  true,
				AgileAggsEnabled:           "true",
				AgileAggsEnabledConverted:  true,
				DualCaseCheck:              "true",
				DualCaseCheckConverted:     true,
				Log:                        common.LogConfig{LogPrefix: "", LogFileRotationSizeMB: 100, CompressLogFile: false},
				Tracing:                    common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 1},
				UseNewQueryPipeline:        "true",
				UseNewPipelineConverted:    true,
				EnableSortIndex:            utils.DefaultValue(false),
				MemoryConfig: common.MemoryConfig{
					MaxUsagePercent: 80,
					LowMemoryMode:   utils.DefaultValue(false),
					SearchPercent:   DEFAULT_SEG_SEARCH_MEM_PERCENT,
					CMIPercent:      DEFAULT_ROTATED_CMI_MEM_PERCENT,
					MetadataPercent: DEFAULT_METADATA_MEM_PERCENT,
					MetricsPercent:  DEFAULT_METRICS_MEM_PERCENT,
					BytesPerQuery:   DEFAULT_BYTES_PER_QUERY,
				},
				MaxOpenColumns:   DEFAULT_MAX_OPEN_COLUMNS,
				QueryTimeoutSecs: DEFAULT_TIMEOUT_SECONDS,
			},
		},
		{ // case 4 - For no input, pick defaults
			[]byte(`
a: b
`),
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
				S3IngestQueueName:           "",
				S3IngestQueueRegion:         "",
				S3IngestBufferSize:          1000,
				MaxParallelS3IngestBuffers:  10,
				QueryHostname:               "localhost:5122",
				PProfEnabled:                "true",
				PProfEnabledConverted:       true,
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
				CompressStatic:              "true",
				CompressStaticConverted:     true,
				Tracing:                     common.TracingConfig{Endpoint: "", ServiceName: "siglens", SamplingPercentage: 0},
				UseNewQueryPipeline:         "true",
				UseNewPipelineConverted:     true,
				EnableSortIndex:             utils.DefaultValue(false),
				MemoryConfig: common.MemoryConfig{
					MaxUsagePercent: 80,
					LowMemoryMode:   utils.DefaultValue(false),
					SearchPercent:   DEFAULT_SEG_SEARCH_MEM_PERCENT,
					CMIPercent:      DEFAULT_ROTATED_CMI_MEM_PERCENT,
					MetadataPercent: DEFAULT_METADATA_MEM_PERCENT,
					MetricsPercent:  DEFAULT_METRICS_MEM_PERCENT,
					BytesPerQuery:   DEFAULT_BYTES_PER_QUERY,
				},
				MaxOpenColumns:   DEFAULT_MAX_OPEN_COLUMNS,
				QueryTimeoutSecs: DEFAULT_TIMEOUT_SECONDS,
			},
		},
	}
	for i, test := range cases {
		actualConfig, err := ExtractConfigData(test.input)
		if i == 2 {
			assert.Error(t, err)
			continue
		}
		if segutils.ConvertUintBytesToMB(memory.TotalMemory()) < SIZE_8GB_IN_MB {
			assert.Equal(t, uint64(50), actualConfig.MemoryConfig.MaxUsagePercent)
			// If memory is less than 8GB, config by default returns 50% as the threshold
			// For testing purpose resetting it to 80%
			actualConfig.MemoryConfig.MaxUsagePercent = 80
		}
		assert.NoError(t, err, fmt.Sprintf("Comparison failed, test=%v", i+1))
		assert.EqualValues(t, test.expected, actualConfig, fmt.Sprintf("Comparison failed, test=%v", i+1))
	}
}

func Test_GetBaseSegDir(t *testing.T) {
	dataPath := t.TempDir()
	InitializeDefaultConfig(dataPath)
	virtualTableName := "evts"
	streamid := "10005995996882630313"
	nextsuff_idx := uint64(1)
	basedir := GetBaseSegDir(streamid, virtualTableName, nextsuff_idx)
	assert.EqualValues(t, dataPath+"/"+GetHostID()+"/final/"+virtualTableName+"/"+streamid+"/1/", basedir)
}
