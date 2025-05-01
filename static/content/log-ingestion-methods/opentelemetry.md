# Open Telemetry
_Ingesting logs into Siglens using OpenTelemetry_

### 1. Pull OTEL Collector Docker Image

Pull the latest Docker image for OpenTelemetry Collector Contrib:

```bash
docker pull otel/opentelemetry-collector-contrib:latest
```

### 2. Configure OTEL Collector

Download the `2kevents.json` file if you are looking for a sample log file:
```bash
curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz
```

Create a config file:

```yaml  title="otelconfig.yaml"
receivers:
  filelog:
    include: [ /var/log/*.log ]  # replace with your log file path

processors:
  batch:

exporters:
  elasticsearch:
    endpoints: ["http://host.docker.internal:8081/elastic"]
    logs_index: "logs-%{+yyyy.MM.dd}"

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [elasticsearch]
```
For in-depth information on OpenTelemetry Collector Contrib configuration, visit the [official OpenTelemetry Collector Contrib documentation](https://opentelemetry.io/docs/collector/).
### 3. Run OTEL Collector

```bash
docker run -v <path_to_your_otel_config_directory>:/etc/otel -v <path_to_your_log_directory>:/var/log -p 4317:4317 -p 8888:8888 otel/opentelemetry-collector-contrib:latest --config /etc/otel/<your_config_file>
```

```bash title="Example command"
docker run -v $HOME/otel:/etc/otel -v /var/log:/var/log -p 4317:4317 -p 8888:8888 otel/opentelemetry-collector-contrib:latest --config /etc/otel/otelconfig.yaml
```
:::note
4317 is the default port for the OTLP gRPC receiver, and 8888 is used for metrics exposition. If you're using different ports in your setup, replace these with your actual ports.
:::