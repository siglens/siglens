# OpenTelemetry

_Ingesting metrics into Siglens using OpenTelemetry_

### 1. Install OpenTelemetry Collector

Pull the docker image for OTEL Collector:

```bash
docker pull otel/opentelemetry-collector
```

### 2. Configure OpenTelemetry Collector

_Note: This sample configuration file is for exporting system metrics to Siglens._

```yml title="otel_collector_config.yaml"
receivers:
  hostmetrics:
    collection_interval: 10s
    scrapers:
      cpu:
      memory:
      disk:
      network:

exporters:
  prometheusremotewrite:
    endpoint: "http://localhost:8081/promql/api/v1/write"
    # If Siglens is running on the host machine where your OTEL Docker container is running, then use `host.docker.internal:8081`.

processors:
  batch:
    send_batch_size: 5000
    timeout: 10s

service:
  pipelines:
    metrics:
      receivers: [hostmetrics]
      processors: [batch]
      exporters: [prometheusremotewrite]
```

You can configure OpenTelemetry to collect different types of metrics according to your needs. For more information on configuring OpenTelemetry, please refer to the [OpenTelemetry Collector Documentation](https://opentelemetry.io/docs/collector/configuration)


### 3. Run OpenTelemetry Collector

```bash 
docker run --rm \
  -v "${PWD}/otel_collector_config.yaml:/etc/otel/config.yaml" \
  otel/opentelemetry-collector \
  --config /etc/otel/config.yaml
```