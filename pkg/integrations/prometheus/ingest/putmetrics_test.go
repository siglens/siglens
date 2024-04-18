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
