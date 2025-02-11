# SigLens

<p align="center">
  <p align="center">SigLensは、SplunkやElasticよりも効率的なオープンソースの可観測性ソリューションであり、💥💥 <b>100倍</b> 💥💥 効率的です。</p>
  <p align="center">SigLensは、Logs 🎯, Metrics 🎯, Traces 🎯 のための<b>単一バイナリ</b>ソリューションです。</p>
  <p align="center">SigLensを使用することで、可観測性ソリューションのコストを⚡⚡ <b>90%</b> ⚡⚡ 削減できます。</p>
</p>

## なぜSigLensを選ぶのか：
10,000人以上のエンジニアに可観測性ツールを提供してきた経験から、いくつかのことを学びました：

- 開発者は、ログ、メトリクス、トレースを確認するためにさまざまなツールを切り替える必要があります 🏃💦 
- Splunk、DataDog、NewRelicの費用は非常に高額です 💸 💸 💸 
- ElasticSearchは多くのマシンを必要とし、クラスターのメンテナンスが難しいです 👩‍💻👩‍💻
- Grafana Lokiのクエリパフォーマンスは遅いです 🐌🐌

監視分野での数十年の経験を活かし、ログ、メトリクス、トレースに特化した可観測性データベースをゼロから構築しました。**`外部依存関係ゼロ`**で、**`単一バイナリ`**で、ラップトップ上で実行でき、**`1日あたり8TB`**のデータを処理できます。

<br /><br />

## コミュニティに参加する
[Slack](https://www.siglens.com/slack)で私たちに挨拶してください 👋

<br />
<br />

# はじめに

### Gitでのインストール
<a href="https://siglens.github.io/siglens-docs/installation/git" target="_blank">こちら</a>に記載されている手順に従ってインストールしてください。

### Dockerでのインストール
<a href="https://siglens.github.io/siglens-docs/installation/docker" target="_blank">こちら</a>に記載されている手順に従ってインストールしてください。

### Helmでのインストール
<a href="https://siglens.github.io/siglens-docs/installation/helm" target="_blank">こちら</a>に記載されている手順に従ってインストールしてください。
<br />

# 特徴：

1. 複数のインジェスト形式をサポート：Open Telemetry、Elastic、Splunk HEC、Loki
2. 複数のクエリ言語をサポート：Splunk SPL、SQL、Loki LogQL
3. シンプルなアーキテクチャで、簡単に始められます

# 利点

### SigLens v/s Splunk, Elastic, Loki

SigLensは、`32台のEC2インスタンス`で1PB/日のデータをインジェストできるのに対し、Splunk、Elastic、Grafana Lokiは`3000台のEC2インスタンス`を必要とします。詳細はこの[ブログ](https://www.sigscalr.io/blog/petabyte-of-observability-data.html)をご覧ください。

### SigLens v/s Elasticsearch

SigLensはElasticsearchよりも**`8倍`**高速です 🚀🚀。詳細はこの[ブログ](https://www.sigscalr.io/blog/sigscalr-vs-elasticsearch.html)をご覧ください。

### SigLens v/s ClickHouse

SigLensはClickHouseよりも**`4〜37倍`**高速です 🚀🚀。詳細はこの[ブログ](https://www.sigscalr.io/blog/sigscalr-vs-clickhouse.html)をご覧ください。

# 貢献

貢献を始める前に、[CONTRIBUTING.md](CONTRIBUTING.md)をお読みください。

# 使用例

#### ログの検索

![Searching Logs](./static/assets/readme-assets/log-searching.png)

#### ダッシュボードの作成

![Creating Dashboards](./static/assets/readme-assets/dashboards.png)

#### アラートの作成

![Creating Alerts](./static/assets/readme-assets/alerting.png)

#### ライブテール

![Live Tail](./static/assets/readme-assets/live-tail.png)

#### ミニオンサーチ

![Minion Searches](./static/assets/readme-assets/minion-searches.png)

## 行動規範

未定
