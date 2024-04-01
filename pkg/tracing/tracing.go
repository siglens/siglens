package tracing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	traceNoop "go.opentelemetry.io/otel/trace/noop"
)

type customExporter struct {
	exporter     *otlptrace.Exporter
	failureCount int
	lock         sync.Mutex
}

func (ce *customExporter) disableTracingTemporarily(ctx context.Context, serviceName string) {
	fmt.Println("Disabling tracing due to repeated export failures.")
	log.Errorf("Disabling tracing due to repeated export failures.")

	// Disable tracing immediately
	config.SetTracingEnabled(false)
	otel.SetTracerProvider(traceNoop.NewTracerProvider())

	if shutdownErr := ce.Shutdown(ctx); shutdownErr != nil {
		log.Errorf("Failed to shut down the tracer provider: %v", shutdownErr)
	}

	// Start a timer to re-enable tracing after one hour
	time.AfterFunc(1*time.Hour, func() {
		fmt.Println("Attempting to re-enable tracing...")
		log.Info("Attempting to re-enable tracing...")
		// Re-enable tracing
		config.SetTracingEnabled(true)
		// Re-initialize tracing
		InitTracing(serviceName)
	})
}

// Shutdown implements trace.SpanExporter.
func (ce *customExporter) Shutdown(ctx context.Context) error {
	log.Info("Shutting down the exporter")
	return ce.exporter.Shutdown(ctx)
}

func getServiceNameFromSpan(span trace.ReadOnlySpan) string {
	for _, resource := range span.Resource().Attributes() {
		if resource.Key == "service.name" {
			return resource.Value.AsString()
		}
	}
	return config.GetTracingServiceName()
}

func (ce *customExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	err := ce.exporter.ExportSpans(ctx, spans)
	ce.lock.Lock()
	defer ce.lock.Unlock()

	if err != nil {
		ce.failureCount++
		log.Errorf("Traces export failed: %v", err)

		// Check if the failure threshold is exceeded.
		if ce.failureCount >= 3 {
			serviceName := getServiceNameFromSpan(spans[0])
			ce.disableTracingTemporarily(ctx, serviceName)
		}
	} else {
		// Reset failure count on a successful export.
		ce.failureCount = 0
	}

	return err
}

// InitTracing initializes the OpenTelemetry tracing.
func InitTracing(serviceName string) func() {

	if !config.IsTracingEnabled() {
		log.Info("Tracing is disabled")
		return func() {}
	}

	if serviceName == "" {
		log.Errorf("Service name was not provided in config, assuming a default service name as siglens")
		serviceName = "siglens"
	}

	if config.GetTracingEndpoint() == "" {
		log.Errorf("Tracing endpoint is required to initialize tracing. Disabling tracing. Please set the endpoint in the config file.")
		config.SetTracingEnabled(false)
		return func() {}
	}

	ctx := context.Background()

	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpointURL(config.GetTracingEndpoint()), // Your collector endpoint
	)

	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Errorf("Failed to create the trace exporter: %v", err)
	}

	customExporter := &customExporter{
		exporter: exporter,
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		log.Errorf("Failed to create resource: %v", err)
	}

	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.TraceIDRatioBased(config.GetTraceSamplingPercentage()/100)),
		trace.WithBatcher(customExporter),
		trace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Errorf("Failed to shut down tracer provider: %v", err)
		}
	}
}

// traceMiddleware wraps the fasthttp request handler to start and end a tracing span.
func TraceMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if !config.IsTracingEnabled() {
			next(ctx)
			return
		}

		tracer := otel.GetTracerProvider().Tracer("fasthttp-server")
		ctxWithSpan, span := tracer.Start(context.Background(), fmt.Sprintf("%s %s", string(ctx.Method()), string(ctx.Path())))
		defer span.End()

		span.SetAttributes(
			attribute.String(string(semconv.HTTPMethodKey), string(ctx.Method())),
			attribute.String(string(semconv.HTTPRequestMethodKey), string(ctx.Method())),
			attribute.String(string(semconv.HTTPURLKey), string(ctx.URI().String())),
			attribute.String(string(semconv.HTTPRouteKey), string(ctx.Path())),
			attribute.String(string(semconv.HostNameKey), string(ctx.Host())),
			attribute.Int(string(semconv.HTTPStatusCodeKey), ctx.Response.StatusCode()),
			attribute.String(string(semconv.HTTPUserAgentKey), string(ctx.Request.Header.UserAgent())),
			attribute.String(string(semconv.HTTPSchemeKey), string(ctx.Request.URI().Scheme())),
			attribute.String(string(semconv.HTTPFlavorKey), string(ctx.Request.Header.Protocol())),
			attribute.String(string(semconv.HostIPKey), string(ctx.RemoteIP().String())),
			attribute.String(string(semconv.HTTPRequestBodySizeKey), fmt.Sprintf("%d", len(ctx.Request.Body()))),
			attribute.String("http.request.body_stringify", string(ctx.Request.Body())),
		)

		// Propagate the new context with the span into the next handler
		ctx.SetUserValue("traceContext", ctxWithSpan)

		next(ctx)

		span.SetAttributes(attribute.Int("http.status_code", ctx.Response.StatusCode()))
	}
}
