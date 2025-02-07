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

package writer

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// FixtureSamplePayload returns a Snappy-compressed TimeSeries
func FixtureSamplePayload(inc int) []byte {
	nameLabelPair := prompb.Label{Name: model.MetricNameLabel, Value: "mah-test-metric"}
	stubLabelPair := prompb.Label{Name: "environment", Value: "production"}
	stubSample := prompb.Sample{Value: 123.45, Timestamp: time.Now().UTC().Unix() + int64(inc)}
	stubTimeSeries := prompb.TimeSeries{
		Labels:  []prompb.Label{stubLabelPair, nameLabelPair},
		Samples: []prompb.Sample{stubSample},
	}

	writeRequest := prompb.WriteRequest{Timeseries: []prompb.TimeSeries{stubTimeSeries}}

	protoBytes, _ := proto.Marshal(&writeRequest)
	compressedBytes := snappy.Encode(nil, protoBytes)
	return compressedBytes
}

func Test_PutMetrics(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	writer.InitWriterNode()

	sTime := time.Now()
	totalSuccess := uint64(0)
	for i := 0; i < 100; i++ {
		postData := FixtureSamplePayload(i)
		success, fail, err := HandlePutMetrics(postData, 0)
		assert.NoError(t, err)
		assert.Equal(t, success, uint64(1))
		assert.Equal(t, fail, uint64(0))
		atomic.AddUint64(&totalSuccess, success)
	}
	log.Infof("Ingested %+v metrics in %+v", totalSuccess, time.Since(sTime))
	err := os.RemoveAll(config.GetDataPath())
	assert.NoError(t, err)
}

var prombMarshalDataSlice [][]byte = [][]byte{
	// nested quotes
	[]byte(`{"metric":{"__name__":"httpcheck_error","error_message":"Get \"http://frontend-proxy:8080\": dial tcp 172.18.0.26:8080: connect: connection refused","http_url":"http://frontend-proxy:8080"},"value":[1738943658.675,"1"]}`),
	// tag with slice values
	[]byte(`{"metric":{"__name__":"target_info","container_id":"bd8e4092ce8daaf26c72a454c4945775d16a4edf1582df59b4ea70302f881468","docker_cli_cobra_command_path":"docker compose","host_arch":"aarch64","host_name":"bd8e4092ce8d","job":"quote","os_description":"6.12.5-linuxkit","os_name":"Linux","os_type":"linux","os_version":"#1 SMP Tue Jan 21 10:23:32 UTC 2025","process_command":"public/index.php","process_command_args":"[\"public/index.php\"]","process_executable_path":"/usr/local/bin/php","process_owner":"www-data","process_pid":"1","process_runtime_name":"cli","process_runtime_version":"8.3.16","service_version":"1.0.0+no-version-set","telemetry_distro_name":"opentelemetry-php-instrumentation","telemetry_distro_version":"1.1.2","telemetry_sdk_language":"php","telemetry_sdk_name":"opentelemetry","telemetry_sdk_version":"1.2.0"},"value":[1738943729.308,"1"]}`),
	[]byte(`{"metric":{"__name__":"target_info","container_id":"cd4ba3a39b41a843d04e83f40014c0d4b0a2c56f68fb339e5e1ffc3fd9975866","docker_cli_cobra_command_path":"docker compose","host_arch":"arm64","host_name":"cd4ba3a39b41","job":"payment","os_type":"linux","os_version":"6.12.5-linuxkit","process_command":"/usr/src/app/index.js","process_command_args":"[\"/usr/local/bin/node\",\"--require\",\"./opentelemetry.js\",\"/usr/src/app/index.js\"]","process_executable_name":"node","process_executable_path":"/usr/local/bin/node","process_owner":"node","process_pid":"18","process_runtime_description":"Node.js","process_runtime_name":"nodejs","process_runtime_version":"22.13.1","telemetry_sdk_language":"nodejs","telemetry_sdk_name":"opentelemetry","telemetry_sdk_version":"1.30.1"},"value":[1738943749.26,"1"]}`),
}

func Test_ConvertToOTtsdbAndAddTSEntry(t *testing.T) {
	timestamp := time.Now().Unix()
	expectedTimestamp := uint32(timestamp)

	for _, data := range prombMarshalDataSlice {

		var dataMap map[string]interface{}
		err := json.Unmarshal(data, &dataMap)
		assert.NoError(t, err)

		value := dataMap["value"].([]interface{})[0].(float64)
		metricName := dataMap["metric"].(map[string]interface{})["__name__"].(string)

		otsdbData, err := ConvertToOTSDBFormat(data, timestamp, value)
		fmt.Println(string(otsdbData))
		assert.NoError(t, err)
		assert.NotNil(t, otsdbData)

		tagsHolder := metrics.GetTagsHolder()
		mName, dp, ts, err := metrics.ExtractOTSDBPayload(otsdbData, tagsHolder)
		assert.NoError(t, err)
		assert.NotNil(t, mName)
		assert.Equal(t, metricName, string(mName))
		assert.Equal(t, value, dp)
		assert.Equal(t, expectedTimestamp, ts)

		tagEntries := tagsHolder.GetEntries()
		assert.NotEmpty(t, tagEntries)
		for _, entry := range tagEntries {
			key := entry.GetTagKey()
			assert.NotEmpty(t, key)

			value := entry.GetTagValue()
			assert.NotEmpty(t, value)

			expectedValue, ok := dataMap["metric"].(map[string]interface{})[key].(string)
			assert.True(t, ok)
			// Since the strings will be escaped, we need to remove the escape characters
			assert.Equal(t, strings.ReplaceAll(expectedValue, `\`, ``), strings.ReplaceAll(string(value), `\`, ``))
		}
	}
}
