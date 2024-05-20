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

package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/liangyaopei/hyper"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastrand"
)

var metricsHLL = hyper.New(10, true)

type MetricsGenerator struct {
	nMetrics               uint32
	f                      *gofakeit.Faker
	val                    float64
	multiTimestampsEnabled bool // Generate multiple timestamps for benchmark tests
}

func InitMetricsGenerator(nmetrics int, gentype string) (*MetricsGenerator, error) {

	multiTimestampsEnabled := false
	seed := int64(fastrand.Uint32n(1_000))
	switch gentype {
	case "", "static":
		log.Infof("Initializing static reader")
	case "dynamic-user":
		log.Infof("Initializing static reader")
	case "benchmark":
		log.Infof("Initializing benchmark reader")
		seed = int64(1001)
		multiTimestampsEnabled = true
	default:
		return nil, fmt.Errorf("unsupported reader type %s. Options=[static,benchmark]", gentype)
	}

	return &MetricsGenerator{
		nMetrics:               uint32(nmetrics),
		f:                      gofakeit.NewUnlocked(seed),
		val:                    0,
		multiTimestampsEnabled: multiTimestampsEnabled,
	}, nil
}

func (mg *MetricsGenerator) Init(fName ...string) error {
	return nil
}

func (mg *MetricsGenerator) GetLogLine() ([]byte, error) {
	return nil, fmt.Errorf("metrics generator can only be used with GetRawLog")
}

func (mg *MetricsGenerator) GetRawLog() (map[string]interface{}, error) {
	retVal := make(map[string]interface{})
	mName := fmt.Sprintf("testmetric%d", mg.f.Rand.Intn(int(mg.nMetrics)))
	retVal["metric"] = mName

	curTime := time.Now().Unix()
	// When the data is for testing purposes, generate three different timestamps (2 days ago, 1 day ago, now) for metrics testing
	if mg.multiTimestampsEnabled {
		switch mg.f.Rand.Int() % 3 {
		case 0:
			retVal["timestamp"] = curTime
		case 1:
			retVal["timestamp"] = curTime - 60*60*24*1
		case 2:
			retVal["timestamp"] = curTime - 60*60*24*2
		}
	} else {
		retVal["timestamp"] = curTime
	}

	retVal["value"] = mg.f.Rand.Float64()*10000 - 5000

	var str strings.Builder
	str.WriteString(mName)

	tags := make(map[string]interface{})

	sColor := mg.f.SafeColor()
	tags["color"] = sColor
	str.WriteString(sColor)

	group := fmt.Sprintf("group %d", mg.f.Rand.Intn(2))
	tags["group"] = group
	str.WriteString(group)

	c := mg.f.Car()
	tags["car_type"] = c.Type
	str.WriteString(c.Type)

	tags["fuel_type"] = c.Fuel
	str.WriteString(c.Fuel)

	tags["model"] = c.Model
	str.WriteString(c.Model)

	retVal["tags"] = tags

	finalStr := str.String()
	metricsHLL.AddString(finalStr)

	return retVal, nil
}

func GetMetricsHLL() uint64 {
	return metricsHLL.Count()
}
