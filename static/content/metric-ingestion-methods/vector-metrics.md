# Vector Metrics

_Ingesting metrics into Siglens using Vector_

### 1. Install Vector

Begin by installing Vector using the instructions provided [here](../log-ingestion/vector.md#1-installation). Once installed, you can refer back to this guide for configuration and starting Vector.

### 2. Configure Vector Metrics

Create a vector config file with the Siglens Vector sink.

_Note: This sample configuration file is for exporting Nginx metrics to Siglens._

```yml title="vector.yaml"
# The directory used for persisting Vector state, such as on-disk buffers, file checkpoints, and more. Please make sure the Vector project has write permissions to this directory.
data_dir: /var/lib/vector

# Sources Reference
sources:
  nginx_metrics:
    type: 'nginx_metrics'
    # A list of NGINX instances to scrape metrics from.
    # Each endpoint must be a valid HTTP/HTTPS URI pointing to an NGINX instance that has the ngx_http_stub_status_module module enabled.
    endpoints:
      - 'http://127.0.0.1/nginx_status'
    namespace: 'nginx'
    # The interval in seconds to poll each endpoint.
    scrape_interval_secs: 5

sinks:
  siglens:
    type: http
    inputs:
      - nginx_metrics
    uri: http://localhost:8081/otsdb/api/put
    encoding:
      codec: json
```

Please note that you might need to add transforms to your Vector configuration according to the structure of your data to ensure it is processed correctly.

For in-depth information on Vector configuration, visit the [official vector documentation](https://vector.dev/docs/reference/configuration/).


### 3. Run Vector

```bash
vector --config vector.yaml
```