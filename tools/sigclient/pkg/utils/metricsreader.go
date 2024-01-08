package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/liangyaopei/hyper"
	"github.com/valyala/fastrand"
)

var metricsHLL = hyper.New(10, true)

type MetricsGenerator struct {
	nMetrics uint32
	f        *gofakeit.Faker
	val      float64
}

func InitMetricsGenerator(nmetrics int) *MetricsGenerator {
	return &MetricsGenerator{
		nMetrics: uint32(nmetrics),
		f:        gofakeit.NewUnlocked(int64(fastrand.Uint32n(1_000))),
		val:      0,
	}
}

func (mg *MetricsGenerator) Init(fName ...string) error {
	return nil
}

func (mg *MetricsGenerator) GetLogLine() ([]byte, error) {
	return nil, fmt.Errorf("metrics generator can only be used with GetRawLog")
}

func (mg *MetricsGenerator) GetRawLog() (map[string]interface{}, error) {

	retVal := make(map[string]interface{})
	mName := fmt.Sprintf("testmetric%d", fastrand.Uint32n(mg.nMetrics))
	retVal["metric"] = mName
	retVal["timestamp"] = time.Now().Unix()
	if fastrand.Uint32n(1_000)%2 == 0 {
		mg.val = float64(fastrand.Uint32n(1_000))
	}
	retVal["value"] = mg.val

	var str strings.Builder
	str.WriteString(mName)

	tags := make(map[string]interface{})

	sColor := mg.f.SafeColor()
	tags["color"] = sColor
	str.WriteString(sColor)

	group := fmt.Sprintf("group %d", fastrand.Uint32n(2))
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
