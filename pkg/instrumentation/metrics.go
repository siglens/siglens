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

package instrumentation

import (
	"context"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	conf "github.com/siglens/siglens/pkg/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
)

var meter = otel.GetMeterProvider().Meter("siglens")
var ctx = context.Background()
var commonAttributes []attribute.KeyValue

var metricsPkgInitialized bool

func InitMetrics() {
	if metricsPkgInitialized {
		return
	}
	exporter, err := prometheus.New()
	if err != nil {
		log.Errorf("Failed to initialize prometheus exporter: %v", err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	commonAttributes = append(commonAttributes, attribute.String("hostname", conf.GetHostID()))
	registerGaugeCallbacks()

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(":2222", nil)
	}()

	log.Infof("OpenTelemetry Prometheus exporter running on :2222")
	metricsPkgInitialized = true
}

func IncrementInt64Counter(metricName api.Int64Counter, value int64) {
	metricName.Add(ctx, value)
}

func IncrementInt64UpDownCounter(metricName api.Int64UpDownCounter, value int64) {
	metricName.Add(
		ctx,
		value,
	)
}

func IncrementInt64CounterWithLabel(metricName api.Int64Counter, value int64,
	labelKey string, labelVal string) {
	attrs := []attribute.KeyValue{
		attribute.String(labelKey, labelVal),
	}

	metricName.Add(
		ctx,
		value,
		api.WithAttributes(attrs...),
	)
}
