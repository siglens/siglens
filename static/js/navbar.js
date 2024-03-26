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
        <div class="position-relative mb-2">
            <div class="nav-help" title="Help & Support">
                <a href="#" class="nav-links"><span class="icon-help"> </span>

                    <span class="nav-link-text">Help & Support</span></a>
            </div>
            <div class="help-options">
                <div class="menu nav-docs" title="SigLens Documentation">
                    <a href="https://www.siglens.com/siglens-docs/" class="nav-links" target="_blank"><span class="icon-docs"></span><span class="nav-link-text">Documentation</span></a>
                </div>
                <div class="menu nav-slack" title="Join Slack Community">
                    <a href="https://www.siglens.com/slack.html" class="nav-links" target="_blank"><span class="icon-slack"></span><span class="nav-link-text">Join Slack Community</span></a>
                </div>
                <div class="menu nav-linkedin" title="Share on LinkedIn">
                    <a href="https://www.linkedin.com/sharing/share-offsite/?url=https://siglens.com" class="nav-links" target="_blank"><span class="icon-linkedin"></span><span class="nav-link-text">Share on LinkedIn</span></a>
                </div>
                <div class="menu nav-twitter" title="Share on Twitter">
                    <a href="https://twitter.com/intent/post?text=Checkout%20SigLens%2C%20industry%27s%20fastest%20observability%20solution%2C%201025x%20faster%20than%20ElasticSearch%2C%2054x%20faster%20than%20ClickHouse%20and%20it%20is%20open%20source.%20https%3A%2F%2Fsiglens.com%20%2C%20%23opensource%2C%20%23observability%20%23logmanagement%20via%20%40siglensHQ" 
                    class="nav-links" target="_blank"><span class="icon-twitter"></span><span class="nav-link-text">Share on Twitter</span></a>
                </div>
                <hr>
                <div class="menu nav-feedback" title="Feedback">
                    <a href="https://docs.google.com/forms/d/e/1FAIpQLSfs_mxeX4LKbjAdX22cOknFaoi2TJcoOGD3OKj2RmZl7evD6A/viewform"
                        target="_blank" class="nav-links">
                        <span class="icon-feedback"></span><span class="nav-link-text feedback">Feedback</span>
                    </a>
                </div>
            </div>
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
    { name: 'Log Sources', url: './test-data.html', class : 'test-data' },
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
        if ($('.subsection-navbar').length) {
            $('.subsection-navbar').appendOrgNavTabs("Tracing", tracingUpperNavTabs);
        }        
    } else if (currentUrl.includes("test-data.html")) {
        $(".nav-ingest").addClass("active");
        $('.ingestion-nav-tab').appendOrgNavTabs("Ingestion", ingestionUpperNavTabs);
    }

    $(".nav-help").on("click", function(event) {
        event.stopPropagation();
        event.preventDefault();
        $(".help-options").slideToggle(200);
    });

    $(document).on("click", function(event) {
        var helpOptions = $(".help-options");
        var menu = $(".nav-help");
        
        if (!menu.is(event.target) && !helpOptions.is(event.target) && helpOptions.has(event.target).length === 0) {
            helpOptions.slideUp(200);
        }
    });

    $(".help-options").on("click", "a", function(event) {
        $(".help-options").slideUp(200);
    });
});


