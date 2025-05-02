import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# OpenTelemetry Collector

_Ingesting logs into Siglens using OpenTelemetry_

<Tabs
  className="bg-light"
  defaultValue="docker"
  values={[
    {label: 'Docker', value: 'docker'},
    {label: 'Helm', value: 'helm'},
  ]
}>

<TabItem value="docker">


### 1. Pull the OpenTelemetry Collector Docker image

```bash
docker pull otel/opentelemetry-collector:latest
```

### 2. Create OpenTelemetry Collector Config

Create a file named otel-collector-config.yml:

```yml title="otel-collector-config.yml"
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"
      http:
        endpoint: "0.0.0.0:4318"

processors:
  batch:
    send_batch_size: 5000
    timeout: 10s

exporters:
  otlphttp/siglens:
    endpoint: "http://host.docker.internal:8081/otlp"
    tls:
      insecure: true
      
service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/siglens]
```

### 3. Create a Docker Compose file

Save this as docker-compose.yml:

```yml title="docker-compose.yml"
version: '3.8'
services:
  otel-collector:
    image: otel/opentelemetry-collector:latest
    container_name: otel-collector
    volumes:
      - ./otel-collector-config.yml:/etc/otel-collector-config.yml
    command: ["--config=/etc/otel-collector-config.yml"]
    ports:
      - "4317:4317"  # gRPC
      - "4318:4318"  # HTTP
```

Or run the container directly:

```bash
docker run --rm -it \
  -v "$(pwd)/otel-config.yml":/otel-local-config.yml \
  -p 4317:4317 \
  -p 4318:4318 \
  otel/opentelemetry-collector:latest \
  --config /otel-local-config.yml
```
</TabItem>

<TabItem value="helm">

### 1. Add the Helm repo

```bash
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm repo update
```

### 2. Create a otel-values.yml file

This file defines the configuration for the collector.

```yml title="otel-values.yml"
mode: deployment

config:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: "0.0.0.0:4317"
        http:
          endpoint: "0.0.0.0:4318"

  exporters:
    otlphttp/siglens:
      endpoint: "http://localhost:8081/otlp"
      tls:
        insecure: true

  processors:
    batch:
      send_batch_size: 5000
      timeout: 10s

  service:
    pipelines:
      logs:
        receivers: [otlp]
        processors: [batch]
        exporters: [otlphttp/siglens]
```

### 3. Install the chart with your custom config

```bash
helm install otel-collector open-telemetry/opentelemetry-collector -f otel-values.yml
```

### 4. Confirm it's running

```bash
kubectl get pods
```
</TabItem>
</Tabs>

:::note
4317 is the default port for the OTLP gRPC receiver, and 8888 is used for metrics exposition. If you're using different ports in your setup, replace these with your actual ports.
:::
