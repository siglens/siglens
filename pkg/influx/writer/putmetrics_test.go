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

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var rawCSV = []byte("measurement,tag1=val1,tag2=val2 value=100 0000000000000000000\nmeasurement,tag1=val1,tag2=val2 value=300 0000000000000000000\n")

func Test_InsertCsv(t *testing.T) {

	config.InitializeTestingConfig()
	writer.InitWriterNode()

	sTime := time.Now()
	totalSuccess := uint64(0)
	for i := 0; i < 100; i++ {
		success, fail, err := HandlePutMetrics([]byte(rawCSV), 0)
		assert.NoError(t, err)
		assert.Equal(t, success, uint64(2))
		assert.Equal(t, fail, uint64(0))
		atomic.AddUint64(&totalSuccess, success)

	}
	log.Infof("Ingested %+v metrics in %+v", totalSuccess, time.Since(sTime))
	err := os.RemoveAll(config.GetDataPath())
	assert.NoError(t, err)

}
