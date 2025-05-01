# Fluentd

*Migrating from Elastic Search to Siglens using Fluentd*

## 1. Install Fluentd

- Follow the instructions to install the Fluentd package from the [official docs](https://docs.fluentd.org/installation).

    - [Debian/Ubuntu](https://docs.fluentd.org/installation/install-by-deb#installing-fluent-package)
    - [macOS](https://docs.fluentd.org/installation/obsolete-installation/treasure-agent-v4-installation/install-by-dmg-td-agent-v4)
    - [Windows](https://docs.fluentd.org/installation/install-by-msi)

### Setup for Elasticsearch version 7.9.3

Fluentd comes with various plugins, including an Elasticsearch plugin (`fluent-plugin-elasticsearch`). Follow these steps to install the compatible version:

1. **Uninstall the default Elasticsearch plugin:** Make sure you are either in the fluentd-command-prompt (on Windows) or the path variables are set up or in the fluentd directory.

    ```bash
    fluent-gem uninstall fluent-plugin-elasticsearch
    ```
  - For td-agent:

    ```bash
    td-agent-gem uninstall fluent-plugin-elasticsearch
    ```
2. Install the compatible Elasticsearch plugin (version 4.3.3):

-  ```bash
    fluent-gem install fluent-plugin-elasticsearch -v 4.3.3
    ```

-  ```bash
    sudo td-agent-gem install fluent-plugin-elasticsearch -v 4.3.3
    ```

3. Verify the installation:

-   ```bash
    fluent-gem list | grep fluent-plugin-elasticsearch
    ```
    For td-agent: 

    ```bash
    td-agent-gem list | grep fluent-plugin-elasticsearch
    ```

4. Install the compatible Elasticsearch gem (version 7.9): First, uninstall any installed elasticsearch gem.

    ```bash
    fluent-gem uninstall elasticsearch
    ```
  - For td-agent: 
    ```bash
    td-agent-gem uninstall elasticsearch
    ```

- Then, install the elasticsearch gem version 7.9:

    ```bash
    fluent-gem install elasticsearch -v 7.9
    ```

- For td-agent:
    ```bash
    td-agent-gem install elasticsearch -v 7.9
    ```

_You might need to setup or install ruby modules or development toolkit. If required, it will be automatically prompted and installed._

## 2. Configure Fluentd

### Sample Configuration file

```conf
<source>
  @type tail
  path D:\Siglens\SplunkExport.json
  pos_file D:\Siglens\fluentd_logs\SplunkExport1.log.pos
  tag my.logs
  read_from_head true
  <parse>
    @type json
  </parse>
</source>

<match my.logs>
  @type elasticsearch
  host http://localhost:8081/elastic
  logstash_format true
  include_tag_key true
  tag_key @log_name
  verify_es_version_at_startup false
  default_elasticsearch_version 7.9
  request_timeout 45s # defaults to 5s
</match>
```

## 3. Run Fluentd

Navigate to the Fluentd directory and run `fluentd -c <<path-of-fluentd-config>>`. On Linux, prepend the command with `sudo`. If using td-agent, replace `fluentd` with `td-agent`. On Windows, run the command as an Administrator.

- **Linux**: 
    ```bash
    sudo fluentd -c /home/fluentd_config.conf
    ```
- **Windows**: Open the fluentd command prompt as an Administrator and run 
  ```bash
  fluentd -c /home/fluentd_config.conf
  ```