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
