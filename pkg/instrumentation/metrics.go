// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
	log.Info("InitMetrics: Initializing metrics package...")
	exporter, err := prometheus.New()
	if err != nil {
		log.Errorf("InitMetrics: Failed to initialize prometheus exporter with error: %v", err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	commonAttributes = append(commonAttributes, attribute.String("hostname", conf.GetHostID()))
	log.Infof("InitMetrics: Added hostname %s as a common attribute to all metrics", conf.GetHostID())

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(":2222", nil)
		if err != nil {
			log.Errorf("InitMetrics: Failed to start Prometheus exporter on :2222 with error: %v", err)
		}
	}()

	log.Infof("InitMetrics: OpenTelemetry Prometheus exporter running on :2222")
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
