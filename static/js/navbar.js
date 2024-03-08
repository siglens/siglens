/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

let navbarComponent = `
    <div>
        <div class="menu logo" title="">
            <a href="./index.html" class="nav-links"><img class="sslogo" src="./assets/siglens-logo.svg">
            </a>
        </div>
     
        <div class="menu nav-search" title="Logs">
            <a href="./index.html" class="nav-links"><span class="icon-search"></span><span
                    class="nav-link-text">Logs</span></a>
        </div>
        <div class="menu nav-traces" title="Tracing">
            <a href="./service-health.html" class="nav-links"><span class="icon-traces"></span><span
                    class="nav-link-text">Tracing</span></a>
         </div>
        <div class="menu nav-metrics" title="Metrics">
            <a href="./metrics.html" class="nav-links"><span class="icon-metrics"></span><span
                    class="nav-link-text">Metrics</span></a>
        </div>
        <div class="menu nav-live" title="Live Tail">
            <a href="./live-tail.html" class="nav-links"><span class="icon-live"></span><span
                    class="nav-link-text">Live Tail</span></a>
        </div>
        <div class="menu nav-alerts" title="Alerting">
            <a href="./all-alerts.html" class="nav-links"><span class="icon-alerts"></span><span class="nav-link-text">Alerting</span></a>
        </div>
        <div class="menu nav-ldb" title="Dashboards-home">
            <a href="../dashboards-home.html" class="nav-links">
                <span class="icon-launchdb"></span><span class="nav-link-text">Dashboards</span></a>
        </div>
        <div class="menu nav-minion" title="Minion Searches">
            <a href="./minion-searches.html" class="nav-links"><span class="icon-minion"></span><span
                    class="nav-link-text">Minion</span></a>
        </div>
        <div class="menu nav-usq" title="Saved Queries">
            <a href="./saved-queries.html" class="nav-links"><span class="icon-usq"></span><span
                    class="nav-link-text">Saved Queries</span></a>
        </div>
        <div class="menu nav-myorg" title="My Org">
            <a href="./cluster-stats.html" class="nav-links"><span class="icon-myorg"></span><span
                    class="nav-link-text">My Org</span></a>
        </div>
        <div class="menu nav-ingest" title="Ingestion">
            <a href="./test-data.html" class="nav-links"><span class="icon-ingest"></span><span
                    class="nav-link-text">Ingestion</span></a>
        </div>
    </div>
    <div>
        <div>
            <div class="theme-btn-group" title="Theme Selector">
                <button class="btn theme-btn dark-theme" id="theme-btn">
                    <img class="theme-img light" src="./assets/light-mode-inactive.svg"
                        onmouseover="this.src='./assets/light-mode-active.svg';"
                        onmouseout="this.src='assets/light-mode-inactive.svg';">
                    <img class="theme-img dark" src="./assets/dark-mode-inactive.svg"
                        onmouseover="this.src='./assets/dark-mode-active.svg';"
                        onmouseout="this.src='./assets/dark-mode-inactive.svg';">
                </button>
            </div>
        </div>
        <div class="menu nav-feedback" title="Feedback">
            <a href="https://docs.google.com/forms/d/e/1FAIpQLSfs_mxeX4LKbjAdX22cOknFaoi2TJcoOGD3OKj2RmZl7evD6A/viewform"
                target="_blank" class="nav-links">
                <span class="icon-feedback"></span><span class="nav-link-text feedback">Feedback</span>
            </a>
        </div>
    </div>
`

let orgUpperNavTabs = [
    { name: 'Cluster Stats', url: './cluster-stats.html', class: 'cluster-stats' },
    {{ .OrgUpperNavTabs}}
    { name: 'Settings', url: './org-settings.html', class : 'org-settings'},
    { name: 'Version', url: './application-version.html', class: 'application-version'}
];

let tracingUpperNavTabs = [
    { name: 'Service Health', url: './service-health.html', class: 'service-health' },
    { name: 'Search Traces', url: './search-traces.html', class : 'search-traces'},
    { name: 'Dependency Graph', url: './dependency-graph.html', class : 'dependency-graph' },
];

let alertsUpperNavTabs = [
    { name: 'Alert Rules', url: './all-alerts.html', class: 'all-alerts' },
    { name: 'Contact Points', url: './contacts.html', class : 'contacts'},
];

let ingestionUpperNavTabs = [
    { name: 'Test Data', url: './test-data.html', class : 'test-data' },
    { name: 'Vector', url: './vector-ingestion.html', class: 'vector-ingestion' },
    { name: 'Logstash', url: './logstash-ingestion.html', class: 'logstash-ingestion' },
    { name: 'Fluentd', url: './fluentd-ingestion.html', class: 'fluentd-ingestion' },
    { name: 'Filebeat', url: './filebeat-ingestion.html', class: 'filebeat-ingestion' },
    { name: 'Promtail', url: './promtail-ingestion.html', class: 'promtail-ingestion' },
];

$(document).ready(function () {
    $("#app-side-nav").prepend(navbarComponent);
    const currentUrl = window.location.href;
    const navItems = [
        ".nav-search",
        ".nav-metrics",
        ".nav-ldb",
        ".nav-usq", 
        ".nav-alerts", 
        ".nav-myorg",
        ".nav-minion",
        ".nav-live",
        ".nav-traces",
        ".nav-ingest",
    ];
    navItems.forEach((item) => $(item).removeClass("active"));

    if (currentUrl.includes("index.html")) {
        $(".nav-search").addClass("active");
    } else if (currentUrl.includes("metrics.html")) {
        $(".nav-metrics").addClass("active");
    } else if (currentUrl.includes("dashboards-home.html") || currentUrl.includes("dashboard.html")) {
        $(".nav-ldb").addClass("active");
    } else if (currentUrl.includes("saved-queries.html")) {
        $(".nav-usq").addClass("active");
    } else if (currentUrl.includes("alerts.html") || currentUrl.includes("alert.html") || currentUrl.includes("alert-details.html")   || currentUrl.includes("contacts.html")){
        $(".nav-alerts").addClass("active");
        $('.alerts-nav-tab').appendOrgNavTabs("Alerting", alertsUpperNavTabs);
    } else if (currentUrl.includes("cluster-stats.html")|| currentUrl.includes("org-settings.html") || currentUrl.includes("application-version.html") {{ .OrgUpperNavUrls}} ) {
        $(".nav-myorg").addClass("active");
        $('.org-nav-tab').appendOrgNavTabs("My Org", orgUpperNavTabs);
    } else if (currentUrl.includes("minion-searches.html")) {
        $(".nav-minion").addClass("active");
    } else if (currentUrl.includes("live-tail.html")) {
        $(".nav-live").addClass("active");
    } else if (currentUrl.includes("service-health.html")|| currentUrl.includes("service-health-overview.html") || currentUrl.includes("dependency-graph.html")|| currentUrl.includes("search-traces.html")) {
        $(".nav-traces").addClass("active");
        $('.subsection-navbar').appendOrgNavTabs("Tracing", tracingUpperNavTabs);
    } else if (currentUrl.includes("test-data.html") || currentUrl.includes("logstash-ingestion.html")|| currentUrl.includes("vector-ingestion.html")|| currentUrl.includes("promtail-ingestion.html")|| currentUrl.includes("filebeat-ingestion.html")|| currentUrl.includes("fluentd-ingestion.html")) {
        $(".nav-ingest").addClass("active");
        $('.ingestion-nav-tab').appendOrgNavTabs("Ingestion", ingestionUpperNavTabs);
    }

});

