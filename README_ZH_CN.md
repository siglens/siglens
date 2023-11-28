# SigLens

<p align="center">


  <p align="center">SigLens 是一个开源的，能够解决可观测性问题的方案，它比 Splunk 和 Elastic 高效 💥💥 <b>100</b> 💥💥 倍。</p>
  <p align="center">SigLens 是一个专为 Logs 🎯, Metrics 🎯 和 Traces 🎯设计的 <b>单个二进制文件</b> 的解决方案。</p>
  <p align="center">使用 SigLens 将为您减少超过 ⚡⚡ <b>90%</b> ⚡⚡ 的可观测性方案开支。</p>

</p>

## 为什么选择 SigLens：
为 10,000 多名工程师提供可观测性工具的经验教会了我们一些事情：

- 开发人员不得不在各种的工具之间切换以查看 logs, metrics 和 traces 🏃💦 
- Splunk, DataDog, NewRelic 的费用非常昂贵 💸 💸 💸 
- ElasticSearch 需要太多的机器，集群维护比较困难 👩‍💻👩‍💻
- Grafana Loki 的查询性能较慢 🐌🐌

凭借几十年的监控领域经验，我们着手开始构建一个全新的，专为 logs, metrics 和 traces 而设计的，并且具有 **`零外部依赖`** 的可观测性数据库。它只需要一个 **`单一的二进制文件`**，就可以在您的笔记本电脑上运行，并且能够 **`每天`** 处理高达 **`8 TB`** 的数据。

<br /><br />

## 加入我们的社区
在 [Slack](https://www.siglens.com/slack) 上和我们打个招呼吧 👋

<br />
<br />

# 入门

### 通过 Git 安装
请按照 <a href="https://siglens.github.io/siglens-docs/installation/git" target="_blank">此处</a> 列出的步骤进行安装。

### 通过 SigLens 二进制文件安装
请按照 <a href="https://siglens.github.io/siglens-docs/installation/binary" target="_blank">此处</a> 列出的步骤进行安装。

### 通过 Docker 安装
请按照 <a href="https://siglens.github.io/siglens-docs/installation/docker" target="_blank">此处</a> 列出的步骤进行安装。

### 通过 Helm 安装
请按照 <a href="https://siglens.github.io/siglens-docs/installation/helm" target="_blank">此处</a> 列出的步骤进行安装。
<br />

# 特点：

1. 支持多种数据引入格式(Ingestion formats)：Open Telemetry, Elastic, Splunk HEC, Loki
2. 支持多种查询语言：Splunk SPL, SQL 和 Loki LogQL
3. 架构简单，易于上手

# 优势

### SigLens v/s Splunk,Elastic,Loki

SigLens 仅使用 `32 个 EC2 实例` 便可以 1 PB/天 的速率引入数据(ingest data)，而 Splunk、Elastic、Grafana Loki 需要 `3000 个 EC2 实例`，详情请查看这篇 [博客(英文)](https://www.sigscalr.io/blog/petabyte-of-observability-data.html)

### SigLens v/s Elasticsearch

SigLens 比 Elasticsearch 快 **`8 倍`** 🚀🚀，详情请查看这篇 [博客(英文)](https://www.sigscalr.io/blog/sigscalr-vs-elasticsearch.html)

### SigLens v/s ClickHouse

SigLens 比 ClickHouse 快 **`4 ~ 37 倍`** 🚀🚀，详情请查看这篇 [博客(英文)](https://www.sigscalr.io/blog/sigscalr-vs-clickhouse.html)

# 贡献

开始贡献之前请阅读 [CONTRIBUTING.md (英文)](CONTRIBUTING.md) 。

# 可用性

#### 搜索日志(Searching Logs)

![Searching Logs](./static/assets/readme-assets/log-searching.png)

#### 创建仪表盘(Creating Dashboards)

![Creating Dashboards](./static/assets/readme-assets/dashboards.png)

#### 创建警报(Creating Alerts)

![Creating Alerts](./static/assets/readme-assets/alerting.png)

#### 实时追踪(Live Tail)

![Live Tail](./static/assets/readme-assets/live-tail.png)

#### 监测搜索(Minion Searches)

![Minion Searches](./static/assets/readme-assets/minion-searches.png)

## 行为准则

待定
