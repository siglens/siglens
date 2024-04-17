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
