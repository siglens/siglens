import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Vector
_Ingesting logs into Siglens using Vector_

### 1. Install Vector

<Tabs
  className="bg-light"
  defaultValue="unix"
  values={[
    {label: 'Linux', value: 'unix'},
    {label: 'macOS', value: 'mac'},
    {label: 'Windows', value: 'windows'},
    {label: 'Other', value:'other'}
  ]
}>

<TabItem value="unix">
Install <a href="https://vector.dev/docs/setup/installation/operating-systems/" target="_blank">Vector</a> for Linux:
<details>
<summary>Debian and Ubuntu</summary>

Add the Vector repo and install using APT:

```bash
curl -1sLf 'https://setup.vector.dev' \
| sudo -E bash
sudo apt-get install vector
```
</details>

<details>
<summary>CentOS, Redhat, and Amazon Linux</summary>

Add the Vector repo and install using YUM:

```bash
curl -1sLf 'https://setup.vector.dev' \
| sudo -E bash
sudo yum install vector
```
</details>

</TabItem>

<TabItem value="mac">

Install <a href="https://vector.dev/docs/setup/installation/operating-systems/macos/" target="_blank">Vector</a> using Homebrew:
```bash
brew tap vectordotdev/brew && brew install vector
```
</TabItem>

<TabItem value="windows">
Install <a href="https://vector.dev/docs/setup/installation/operating-systems/windows/" target="_blank">Vector</a> using the official installer for Windows:
```powershell
powershell Invoke-WebRequest https://packages.timber.io/vector/0.36.1/vector-x64.msi -OutFile vector-0.36.1-x64.msi
msiexec /i vector-0.36.1-x64.msi
```
</TabItem>


<TabItem value="other">
Install <a href="https://vector.dev/docs/setup/installation/manual/vector-installer/" target="_blank">Vector</a> using the Vector installer:
```bash
curl --proto '=https' --tlsv1.2 -sSfL https://sh.vector.dev | bash
```
</TabItem>

</Tabs>

### 2. Configure Vector

Download the sample events file using the following command:
```bash
curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz
```

Create a vector config file with the Siglens Vector sink.

<html>
<Tabs
  defaultValue="yaml"
  values=
  {
    [
      { label: 'YAML', value: 'yaml', },
      { label: 'JSON', value: 'json', },
    ]
  }
>
<TabItem value="yaml">

```yml title="vector.yaml"
data_dir: /var/lib/vector

sources:
  read_from_file:
    type: file
    include:
      - 2kevents.json # Path to the log file

sinks:
  siglens:
    type: elasticsearch
    inputs:
      - read_from_file
    endpoints:
      - http://localhost:8081/elastic/
    mode: bulk
    healthcheck:
      enabled: false
```

</TabItem>

<TabItem value="json">
```json title="vector.json"
{
  "data_dir": "/var/lib/vector",
  "sources": {
    "read_from_file": {
      "type": "file",
      "include": [
        "2kevents.json"
      ]
    }
  },
  "sinks": {
    "siglens": {
      "type": "elasticsearch",
      "inputs": [
        "read_from_file"
      ],
      "endpoint": "http://localhost:8081/elastic/",
      "mode": "bulk",
      "healthcheck": {
        "enabled": false
      }
    }
  }
}
```
</TabItem>
</Tabs>

</html>
Please note that you might need to add transforms to your Vector configuration according to the structure of your data to ensure it is processed correctly.

For in-depth information on Vector configuration, visit the [official vector documentation](https://vector.dev/docs/reference/configuration/).

### 3. Run Vector

```bash
vector --config vector.yaml
```