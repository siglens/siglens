<p align="center">
<img src="https://github.com/siglens/siglens/assets/604069/7dab105b-2102-4a32-85c7-02fbb4604217" width="300">
</p>

---
[![Build Status](https://github.com/siglens/siglens/workflows/siglens-docker-release/badge.svg)](https://github.com/siglens/siglens/actions/workflows/publish-prod-images.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/siglens/siglens)](https://goreportcard.com/report/github.com/siglens/siglens)
[![GoDoc](https://pkg.go.dev/github.com/siglens/siglens)

English | [简体中文](README_ZH_CN.md)

<p align="center">
  

  <p align="left">Open Source Observability that is 💥💥 <b>100x</b> 💥💥 more efficient than Splunk </p>
  <p align="left"><b>Single binary</b> for Logs 🎯, Metrics 🎯 and Traces 🎯.</p>
  <p align="left">Cut down your Splunk bill by ⚡ ⚡ <b>90%</b> ⚡ ⚡ </p>

</p>



# Why SigLens:
Our experience servicing 10,000+ engineers with Observability tools taught us a few things:

- Developers have to jump through different tools for logs, metrics, traces
- Splunk, DataDog, NewRelic are very expensive 💸 💸 💸 
- ElasticSearch takes too many machines, cluster maintenance is hard 👩‍💻👩‍💻
- Grafana Loki has slow query performance 🐌🐌


Armed with decades of experience in monitoring domain, we set out to build a observability DB from the ground up, uniquely suited for logs, metrics and traces with **`zero`** external dependencies. A **`single binary`** that you can run on your laptop and process **`8 TB/day`**.  
<br /><br />


# Setup
## Installation

### &emsp; <a href="https://siglens.github.io/siglens-docs/installation/binary" target="_blank">Binary</a> &emsp; | &emsp; <a href="https://siglens.github.io/siglens-docs/installation/docker" target="_blank">Docker</a> &emsp;| &emsp; <a href="https://siglens.github.io/siglens-docs/installation/helm" target="_blank">Helm</a>

## Documentation
### &emsp; <a href="https://siglens.github.io/siglens-docs" target="_blank">Docs</a> &emsp;


# Differentiators

### SigLens v/s Splunk,Elastic,Loki  
Check out this <a href="https://www.sigscalr.io/blog/petabyte-of-observability-data.html" target="_blank">blog</a> where SigLens ingested data at 1 PB/day rate for 24 hours on a mere `32 EC2 instances` compared to `3000 EC2 instances` required for Splunk, Elastic, Grafana Loki

### SigLens v/s Elasticsearch 
Check out this <a href="https://www.sigscalr.io/blog/sigscalr-vs-elasticsearch.html" target="_blank">blog</a> where SigLens is **`8x`** Faster than Elasticsearch 🚀🚀

### SigLens v/s ClickHouse 
Check out this <a href="https://www.sigscalr.io/blog/sigscalr-vs-clickhouse.html" target="_blank">blog</a> where SigLens is **`4x-37x`** Faster than ClickHouse 🚀🚀


<br />

# Features:

1. Multiple Ingestion formats: Open Telemetry, Elastic, Splunk HEC, Loki
2. Multiple Query Languages: Splunk SPL, SQL and Loki LogQL
3. Simple architecture, easy to get started.


## Join our Community

Have questions, ask them in our community <a href="https://www.siglens.com/slack" target="_blank">Slack</a> 👋

<br />


# Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) to get started with making contributions to SigLens.

# How-Tos

#### Searching Logs
![Searching Logs](./static/assets/readme-assets/log-searching.png)

#### Creating Dashboards
![Creating Dashboards](./static/assets/readme-assets/dashboards.png)

#### Creating Alerts
![Creating Alerts](./static/assets/readme-assets/alerting.png)

#### Live Tail
![Live Tail](./static/assets/readme-assets/live-tail.png)

#### Minion Searches
![Minion Searches](./static/assets/readme-assets/minion-searches.png)


## Code of Conduct
`TBD`


## Steps to create docker image
docker build --build-arg VERSION=0.1.4 -t yourapp .


