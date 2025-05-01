import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Fluent-bit

_Ingesting logs into Siglens using Fluent-bit_

### 1. Install Fluent-bit

Install <a href="https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit" target="_blank">Fluent-bit</a> on your server

### 2. Configure Fluent-bit

Download the sample nginx_logs file using the following command:
```bash
curl -O https://raw.githubusercontent.com/elastic/examples/refs/heads/master/Common%20Data%20Formats/nginx_json_logs/nginx_json_logs
```

Create a fluent-bit.conf file:
```xml title="fluent-bit.conf"
[SERVICE]
    Flush          5
    Daemon         Off
    Log_Level      info
    Parsers_File   parsers.conf  # This file is defined below

[INPUT]
    Name              tail
    Path              /Users/fluent-bit-ingestion/nginx_logs_json.txt
    DB                /tmp/flb.db
    Tag               nginx_logs
    Parser            json_parser
    Refresh_Interval  5
    Read_from_Head    True

[FILTER]
    Name          parser
    Match         nginx_logs
    Key_Name      log
    Parser        json_parser
    Reserve_Data  On

[OUTPUT]
    Name                 es
    Match                nginx_logs
    Host                 localhost
    Port                 8081
    Path                 /elastic
    TLS                  Off
    Index                nginx-logs-fluentbit-es
    Suppress_Type_Name   On
    Logstash_Format      Off
    Retry_Limit          False
```

Create a parsers.conf file
```xml title="parsers.conf"
[PARSER]
    Name        json_parser
    Format      json
    Time_Key    timestamp
    Time_Format %Y-%m-%dT%H:%M:%S
```
For more information on customizing your `fluent-bit.conf` file according to your logs, refer to the [Fluent-bit documentation](https://docs.fluentbit.io/manual/administration/configuring-fluent-bit).

### 3. Run Fluent-bit

Open a terminal, navigate to the directory containing the previously created configuration files, and execute the following command:

```sh
fluent-bit -c ./fluent-bit.conf
```

Ensure that the Fluent-bit configuration file path is correctly set and that the parsers file is located in the same directory.