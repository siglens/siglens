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

package writer

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// FixtureSamplePayload returns a Snappy-compressed TimeSeries
func FixtureSamplePayload() []byte {
	nameLabelPair := prompb.Label{Name: model.MetricNameLabel, Value: "mah-test-metric"}
	stubLabelPair := prompb.Label{Name: "environment", Value: "production"}
	stubSample := prompb.Sample{Value: 123.45, Timestamp: time.Now().UTC().Unix()}
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
	config.InitializeTestingConfig()
	writer.InitWriterNode()
	postData := FixtureSamplePayload()

	sTime := time.Now()
	totalSuccess := uint64(0)
	for i := 0; i < 100; i++ {
		success, fail, err := HandlePutMetrics(postData)
		assert.NoError(t, err)
		assert.Equal(t, success, uint64(1))
		assert.Equal(t, fail, uint64(0))
		atomic.AddUint64(&totalSuccess, success)
	}
	log.Infof("Ingested %+v metrics in %+v", totalSuccess, time.Since(sTime))
	err := os.RemoveAll(config.GetDataPath())
	assert.NoError(t, err)
}
