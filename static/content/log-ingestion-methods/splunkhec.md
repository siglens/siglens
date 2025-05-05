# Fluentd

*Migrating from Splunk to Siglens using Fluentd*

## 1. Install Fluentd

- Follow the instructions to install the Fluentd package from the [official docs](https://docs.fluentd.org/installation).

    - [Debian/Ubuntu](https://docs.fluentd.org/installation/install-by-deb#installing-fluent-package)
    - [macOS](https://docs.fluentd.org/installation/obsolete-installation/treasure-agent-v4-installation/install-by-dmg-td-agent-v4)
    - [Windows](https://docs.fluentd.org/installation/install-by-msi)

### Setup for Splunk

- Fluentd doesn't include a plugin for Splunk by default.
- Install the [`fluent-plugin-splunk-hec`](https://github.com/splunk/fluent-plugin-splunk-hec). You can read more about the Splunk Plugin [here](https://docs.fluentd.org/v/0.12/output/splunk).
- For fluentd: 

  ```bash
  fluent-gem install fluent-plugin-splunk-hec
  ```
  Verify if installation was successful -
  ```bash
  fluent-gem list | grep fluent-plugin-splunk-hec
  ```

- For td-agent:

    ```bash
    sudo td-agent-gem install fluent-plugin-splunk-hec
    ```
    Verify if installation was successful -
    ```bash
    td-agent-gem list | grep fluent-plugin-splunk-hec
    ```

- _You might need to setup or install ruby modules or development toolkit. If required, it will be automatically prompted and installed._

## 2. Configure Fluentd

### Sample Configuration file

```conf
<source>
  @type tail
  path D:\Siglens\SplunkExport.json
  pos_file D:\Siglens\fluentd_logs\SplunkExport2.log.pos
  tag my.logs
  read_from_head true
  <parse>
    @type json
  </parse>
</source>

<match my.logs>
  @type splunk_hec
  host hostname
  hec_token A94A8FE5CCB19BA61C4C08
  hec_host localhost
  hec_port 8081
  hec_endpoint /services/collector/event
  protocol http
  index fluentd-ind-0
  # Buffer configuration
  <buffer>
    chunk_limit_records 1
    flush_at_shutdown true
  </buffer>
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