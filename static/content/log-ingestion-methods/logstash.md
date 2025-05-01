import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Logstash

_Ingesting logs into Siglens using Logstash_

### 1. Install Logstash

<Tabs
  className="bg-light"
  defaultValue="unix"
  values={[
    {label: 'Linux', value: 'unix'},
    {label: 'macOS', value: 'mac'},
    {label: 'Windows', value: 'windows'},
  ]
}>

<TabItem value="unix">

<details>
<summary>Debian and Ubuntu</summary>

Install <a href="https://www.elastic.co/guide/en/logstash/current/installing-logstash.html" target="_blank">Logstash<i class="fas fa-external-link-alt"></i></a> using APT:

```bash
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-7.x.list
sudo apt-get update && sudo apt-get install logstash
```
</details>

<details>
<summary>CentOS, Redhat, and Amazon Linux</summary>

Install <a href="https://www.elastic.co/guide/en/logstash/current/installing-logstash.html" target="_blank">Logstash<i class="fas fa-external-link-alt"></i></a> using YUM:

```bash
sudo rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch
echo "[logstash-7.x]
name=Elastic repository for 7.x packages
baseurl=https://artifacts.elastic.co/packages/7.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md" | sudo tee /etc/yum.repos.d/logstash.repo
sudo yum install logstash
```
</details>

</TabItem>

<TabItem value="mac">
Install <a href="https://www.elastic.co/guide/en/logstash/7.9/installing-logstash.html" target="_blank">Logstash<i class="fas fa-external-link-alt"></i></a> on macOS:

```bash
brew install logstash
```
</TabItem>
<TabItem value="windows">

Install <a href="https://www.elastic.co/guide/en/logstash/current/installing-logstash.html" target="_blank">Logstash <i class="fas fa-external-link-alt"></i></a> using the official installer for Windows:

```bash
# Download and install the Public Signing Key:
wget https://artifacts.elastic.co/GPG-KEY-elasticsearch
rpm --import GPG-KEY-elasticsearch

# Add the repository definition to your /etc/yum.repos.d/ directory:
echo "[logstash-7.x]
name=Elastic repository for 7.x packages
baseurl=https://artifacts.elastic.co/packages/7.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md" | sudo tee /etc/yum.repos.d/logstash.repo

# And finally, install Logstash:
sudo yum install logstash
```
</TabItem>

</Tabs>

### 2. Configure Logstash


Download the sample events file using the following command:
```bash
curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz
```

Create a config file:

```ruby title="logstash.conf"
input {
  file {
    path => "/Users/username/logstash/2kevents.json" # Path to the log file
    start_position => "beginning"
  }
}

filter {
  json {
    source => "message"
    remove_field => ["message", "file", "source_type", "path"]
  }
  mutate {
    add_field => { "index" => "logstash_http" }
  }
  if ![first_name] {
    drop { }
  }
}

output {
  http {
    format => "json"
    content_type => "application/json"
    http_method => "post"
    url => "http://localhost:8081/services/collector/event"
    headers => ['Authorization', 'A94A8FE5CCB19BA61C4C08']
  }
}
```

For more information on customizing your `logstash.conf` file according to your logs, refer to the [Logstash documentation](https://www.elastic.co/guide/en/logstash/current/configuration.html).

### 3. Run Logstash

```bash
sudo logstash -f $(pwd)/logstash.conf
```

Please ensure to replace ```$(pwd)/logstash.conf``` with the absolute path to your Logstash configuration file.