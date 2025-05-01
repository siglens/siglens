import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Promtail

*Ingesting logs into Siglens using Promtail*

### 1. Install Promtail

<Tabs
  className="bg-light"
  defaultValue="unix"
  values={[
    {label: 'Linux', value: 'unix'},
    {label: 'macOS', value: 'mac'},
  ]
}>

<TabItem value="unix">
Install <a href="https://grafana.com/docs/loki/latest/clients/promtail/installation/" target="_blank">Promtail</a> for Linux:

<details>
<summary>Debian and Ubuntu</summary>

Download and install the Promtail binary:

```bash
curl -O -L "https://github.com/grafana/loki/releases/download/v2.9.5/promtail-linux-amd64.zip"
sudo apt install unzip
unzip "promtail-linux-amd64.zip"
sudo chmod a+x "promtail-linux-amd64"
```
</details>

<details>
<summary>CentOS, Redhat, and Amazon Linux</summary>

Download and install the Promtail binary:

```bash
curl -O -L "https://github.com/grafana/loki/releases/download/v2.9.5/promtail-linux-amd64.zip"
sudo yum install unzip
unzip "promtail-linux-amd64.zip"
sudo chmod a+x "promtail-linux-amd64"
```
</details>

</TabItem>

<TabItem value="mac">

Install <a href="https://grafana.com/docs/loki/latest/clients/promtail/installation/" target="_blank">Promtail</a> using Homebrew:
```bash
brew install promtail
```
</TabItem>

</Tabs>

### 2. Configure Promtail

Download the sample events file using the following command:
```bash
curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz
```

Create a config file:

```yml title="promtail.yaml"
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://localhost:8081/loki/api/v1/push
scrape_configs:
- job_name: system
  static_configs:
  - targets:
      - localhost
    labels:
      job: varlogs
      __path__: /var/log/*log # Path to the log file
```
For more information on customizing your `promtail.yaml` file according to your logs, refer to the [Promtail documentation](https://grafana.com/docs/loki/latest/clients/promtail/configuration/).

### 3. Run Promtail

<Tabs
  className="bg-light"
  defaultValue="unix"
  values={[
    {label: 'Linux', value: 'unix'},
    {label: 'macOS', value: 'mac'},
  ]
}>

<TabItem value="unix">

```bash
./promtail-linux-amd64 -config.file=promtail.yaml
```
</TabItem>

<TabItem value="mac">

```bash
promtail -config.file=promtail.yaml
```
</TabItem>
</Tabs>