# SigLens

SigLens is an Open Source Observability solution that is **100x** more efficient than Splunk, Elastic. 

# Why SigLens:
Our experience servicing 10,000+ engineers with Observability tools taught us a few things:

- Developers have to jump through different tools for logs, metrics, traces
- Splunk, DataDog, NewRelic are very expensive
- ElasticSearch takes too many machines, cluster maintainence is hard
- Grafana Loki has slow query performance

Armed with decades of experience in monitoring domain, we set out to build a observability DB from the ground up, uniquely suited for logs, metrics and traces with **`zero`** external dependencies. A **`single binary`** that you can run on your laptop and process `8 TB/day` on it.  
<br /><br />


## Join our Slack community

Come say Hi to us on <a href="https://www.siglens.com/slack" target="_blank">Slack</a> 👋

<br /><br />

## Getting Started

### Install Using Git Repo
```
git clone git@github.com:siglens/siglens
cd siglens
go run cmd/siglens/main.go --config server.yaml
```

### Install Using SigLens Binary
`TBD`

### Install Using SigLens Docker

- SigLens can be installed on Linux or macOS machine. 
- On macOS, Docker Engine should be installed before you run the install script. 
- Git clone the SigLens repository and cd into the siglens directory 
```
    git@github.com:siglens/siglens.git
    cd siglens
```
- Run the install.sh script:
```
    ./install_with_docker.sh
```

The SigLens backend is deployed independently of the UI. 
To allow the UI to connect to the backend a docker network can be used.
```
    wget https://sigscalr-configs.s3.amazonaws.com/1.1.31/server.yaml
    docker pull siglens/siglens:0.1.0 
    mkdir data
    docker run -it --mount type=bind,source="$(pwd)"/data,target=/siglens/data \
        --mount type=bind,source="$(pwd)"/server.yaml,target=/siglens/server.yaml \
        -p 8081:8081 -p 80:80 siglens/siglens:0.1.0 
```
To be able to query data across restarts, set `ssInstanceName` in server.yaml.

The target for the data directory mounting should be the same as the data directory (`dataPath`configuration) in server.yaml

# Features:

1. Multiple Ingestion formats: Open Telemetry, Elastic, Splunk HEC, Loki
2. Multiple Query Languages: Splunk SPL, SQL and Loki LogQL
3. Simple architecture, easy to get started.

# Differentiators

### SigLens v/s Elasticsearch 
Check out this <a href="https://www.sigscalr.io/blog/sigscalr-vs-elasticsearch.html" target="_blank">blog</a> where SigLens is ` 8x ` Faster than Elasticsearch

### SigLens v/s ClickHouse 
Check out this <a href="https://www.sigscalr.io/blog/sigscalr-vs-clickhouse.html" target="_blank">blog</a> where SigLens is `4x-37x` Faster than ClickHouse

### SigLens v/s Splunk,Elastic,Loki  
Check out this <a href="https://www.sigscalr.io/blog/petabyte-of-observability-data.html" target="_blank">blog</a> where SigLens ingested data at 1 PB/day rate for 24 hours on a mere `32 EC2 instances` compared to `3000 EC2 instances` required for Splunk, Elastic, Grafana Loki

# Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) to get started with making contributions to SigLens.

# Usability

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

