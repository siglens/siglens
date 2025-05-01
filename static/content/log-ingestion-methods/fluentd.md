import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Fluentd

_Ingesting logs into Siglens using Fluentd_

### 1. Install Fluentd

Install <a href="https://docs.fluentd.org/installation" target="_blank">Fluentd</a> on your server

### 2. Configure Fluentd

Download the sample events file using the following command:
```bash
curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz
```

Create a fluentd.conf file:
```xml title="fluentd.conf"
<source>
  @type tail
  path /Users/username/logstash/2kevents.json # Path to the log file
  pos_file /Users/username/logstash/2kevents.json.pos  # Path to the position file
  tag my.logs
  read_from_head true
  <parse>
    @type json
  </parse>
</source>

<filter my.logs>
  @type record_transformer
  <record>
    index "fluentd_http"
  </record>
</filter>

<filter my.logs>
  @type grep
  <regexp>
    key first_name
    pattern /.+/
  </regexp>
</filter>

<match my.logs>
  @type http

  endpoint http://127.0.0.1:8081/services/collector/event?source=fluentd_source
  open_timeout 2
  <format>
    @type json
  </format>
  <buffer>
    chunk_limit_records 1
    flush_interval 10s
  </buffer>
</match>
```
For more information on customizing your `fluentd.conf` file according to your logs, refer to the [Fluentd documentation](https://docs.fluentd.org/configuration).

### 3. Run Fluentd

<Tabs
  className="bg-light"
  defaultValue="unix"
  values={[
    {label: 'Linux', value: 'unix'},
    {label: 'macOS', value: 'mac'},
    {label: 'Windows', value: 'windows'}
  ]
}>

<TabItem value="unix">
Navigate to the Fluentd directory and run the following command. If using td-agent, replace `fluentd` with `td-agent`.

```bash
sudo fluentd -c /home/fluentd.conf
```
</TabItem>

<TabItem value="mac">
Navigate to the Fluentd directory and run the following command. If using td-agent, replace `fluentd` with `td-agent`.

```bash
sudo fluentd -c /Users/username/fluentd.conf
```
</TabItem>

<TabItem value="windows">
Open powershell as an Administrator and run the following command. If using td-agent, replace `fluentd` with `td-agent`.

```bash
fluentd -c C:\path\to\fluentd.conf
```
</TabItem>

</Tabs>

Make sure to set the correct path to Fluentd and its config file.