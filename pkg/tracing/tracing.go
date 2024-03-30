package tracing

import (
	"context"
	"fmt"

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
)

// InitTracing initializes the OpenTelemetry tracing.
func InitTracing(serviceName string) func() {

	if !config.IsTracingEnabled() {
		log.Info("Tracing is disabled")
		return func() {}
	}

	if serviceName == "" {
		log.Errorf("Service name is required to initialize tracing")
		serviceName = "unknown"
	}

	if config.GetTracingEndpoint() == "" {
		log.Errorf("Tracing endpoint is required to initialize tracing. Disabling tracing. Please set the endpoint in the config file.")
		config.SetTracingEnabled(false)
		return func() {}
	}

	ctx := context.Background()

	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpointURL(config.GetTracingEndpoint()), // Your collector endpoint
		otlptracehttp.WithInsecure(),                               // Use WithInsecure for non-production environments
	)

	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Fatalf("Failed to create the trace exporter: %v", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create resource: %v", err)
	}

	tp := trace.NewTracerProvider(
		// trace.WithSampler(trace.AlwaysSample()), // Change this to trace.NeverSample() for production
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatalf("Failed to shut down tracer provider: %v", err)
		}
	}
}

// traceMiddleware wraps the fasthttp request handler to start and end a tracing span.
func TraceMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
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
