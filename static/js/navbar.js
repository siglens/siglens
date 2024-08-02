/*
 * Copyright (c) 2021-2024 SigScalr, Inc.
 *
 * This file is part of SigLens Observability Solution
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

let navbarComponent = `
    <div>
        <div class="nav-main-menu logo">
            <a href="./index.html" class="nav-links"><img class="sslogo" src="./assets/siglens-logo.svg">
            </a>
        </div>
     
        <div class="menu nav-search">
            <a href="./index.html" class="nav-links"><span class="icon-search"></span><span
                    class="nav-link-text">Logs</span></a>
        </div>
        <div class="menu nav-traces tracing-dropdown-toggle"  style="display:flex;flex-direction:row">
            <a class="nav-links" href="./service-health.html">
                <span class="icon-traces"></span>
                <span class="nav-link-text">Tracing</span>
            </a>
            <ul class="traces-dropdown navbar-submenu">
                <a href="./service-health.html"><li class="traces-link">Service Health</li></a>
                <a href="./search-traces.html"><li class="traces-link">Search Traces</li></a>
                <a href="./dependency-graph.html"><li class="traces-link">Dependency Graph</li></a>
            </ul>
         </div>
        <div class="menu nav-metrics metrics-dropdown-toggle"  style="display:flex;flex-direction:row">
            <a class="nav-links" href="./metrics-explorer.html">
                <span class="icon-metrics"></span>
                <span class="nav-link-text">Metrics</span>
            </a>
            <ul class="metrics-dropdown navbar-submenu">
                <a href="./metrics-explorer.html"><li class="metrics-summary-metrics-link">Explorer</li></a>
                <a href="./metric-summary.html"><li class="metrics-summary-metrics-link">Summary</li></a>
                <a href="./metric-cardinality.html"><li class="metrics-summary-metrics-link">Cardinality</li></a>
            </ul>
        </div>
        <div class="menu nav-slos">
            <a href="./all-slos.html" class="nav-links"><span class="icon-live"></span><span
                    class="nav-link-text">SLOs</span></a>
        </div>
        <div class="menu nav-alerts">
            <a href="./all-alerts.html" class="nav-links"><span class="icon-alerts"></span><span class="nav-link-text">Alerting</span></a>
        </div>
        <div class="menu nav-ldb">
            <a href="../dashboards-home.html" class="nav-links">
                <span class="icon-launchdb"></span><span class="nav-link-text">Dashboards</span></a>
        </div>
        <div class="menu nav-minion">
            <a href="./minion-searches.html" class="nav-links"><span class="icon-minion"></span><span
                    class="nav-link-text">Minion</span></a>
        </div>
        <div class="menu nav-usq">
            <a href="./saved-queries.html" class="nav-links"><span class="icon-usq"></span><span
                    class="nav-link-text">Saved Queries</span></a>
        </div>
        <div class="menu nav-myorg">
            <a href="./cluster-stats.html" class="nav-links"><span class="icon-myorg"></span><span
                    class="nav-link-text">My Org</span></a>
        </div>
        <div class="menu nav-ingest ingestion-dropdown-toggle"  style="display:flex;flex-direction:row">
            <a class="nav-links" href="./test-data.html">
                <span class="icon-ingest"></span>
                <span class="nav-link-text">Ingestion</span>
            </a>
            <ul class="ingestion-dropdown navbar-submenu">
                <a href="./test-data.html"><li class="ingestion-link">Log Ingestion</li></a>
                <a href="./metrics-ingestion.html"><li class="ingestion-link">Metrics Ingestion</li></a>
                <a href="./traces-ingestion.html"><li class="ingestion-link">Traces Ingestion</li></a>                
            </ul>
        </div>
    </div>
    <div>
        <div>
            <div class="theme-btn-group">
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
            <div class="menu nav-help">
                <a href="#" class="help-links"><span class="icon-help">
                </span><span class="nav-link-text">Help & Support</span></a>
            </div>
            <div class="help-options">
                <div class="nav-docs">
                    <a href="https://www.siglens.com/siglens-docs/"  target="_blank" class="help-links"><span class="icon-docs"></span><span class="nav-link-text">Documentation</span></a>
                </div>
                <div class="nav-slack">
                    <a href="https://www.siglens.com/slack.html"  target="_blank" class="help-links"><span class="icon-slack"></span><span class="nav-link-text">Join Slack Community</span></a>
                </div>
                <div class="nav-linkedin">
                    <a href="https://www.linkedin.com/sharing/share-offsite/?url=https://siglens.com" target="_blank" class="help-links"><span class="icon-linkedin"></span><span class="nav-link-text">Share on LinkedIn</span></a>
                </div>
                <div class="nav-twitter">
                    <a href="https://twitter.com/intent/post?text=Checkout%20SigLens%2C%20industry%27s%20fastest%20observability%20solution%2C%201025x%20faster%20than%20ElasticSearch%2C%2054x%20faster%20than%20ClickHouse%20and%20it%20is%20open%20source.%20https%3A%2F%2Fsiglens.com%20%2C%20%23opensource%2C%20%23observability%20%23logmanagement%20via%20%40siglensHQ" 
                    target="_blank" class="help-links"><span class="icon-twitter"></span><span class="nav-link-text">Share on Twitter</span></a>
                </div>
                <hr>
                <div class="nav-feedback">
                    <a href="https://docs.google.com/forms/d/e/1FAIpQLSfs_mxeX4LKbjAdX22cOknFaoi2TJcoOGD3OKj2RmZl7evD6A/viewform"
                        target="_blank" class="help-links">
                        <span class="icon-feedback"></span><span class="nav-link-text feedback">Feedback</span>
                    </a>
                </div>
            </div>
        </div>
    </div>
`;

let orgUpperNavTabs = [
    { name: 'Cluster Stats', url: './cluster-stats.html', class: 'cluster-stats' },
    {{ .OrgUpperNavTabs }}
    { name: 'Settings', url: './org-settings.html', class: 'org-settings' },
    { name: 'Version', url: './application-version.html', class: 'application-version' },
];

let tracingUpperNavTabs = [
    { name: 'Service Health', url: './service-health.html', class: 'service-health' },
    { name: 'Search Traces', url: './search-traces.html', class: 'search-traces' },
    { name: 'Dependency Graph', url: './dependency-graph.html', class: 'dependency-graph' },
];

let alertsUpperNavTabs = [
    { name: 'Alert Rules', url: './all-alerts.html', class: 'all-alerts' },
    { name: 'Contact Points', url: './contacts.html', class: 'contacts' },
];

$(document).ready(function () {
    $('#app-side-nav').prepend(navbarComponent);
    const currentUrl = window.location.href;


    if (currentUrl.includes('index.html')) {
        $('.nav-search').addClass('active');
    } else if (currentUrl.includes('metrics-explorer.html')) {
        $('.nav-metrics').addClass('active');
        $('.nav-metrics').addClass('disable-hover');
        setTimeout(function () {
            $('.nav-metrics').removeClass('disable-hover');
        }, 500);
    } else if (currentUrl.includes('metric-summary.html')) {
        $('.nav-metrics').addClass('active');
    } else if (currentUrl.includes('metric-cardinality.html')) {
        $('.nav-metrics').addClass('active');
    } else if (currentUrl.includes('dashboards-home.html') || currentUrl.includes('dashboard.html')) {
        $('.nav-ldb').addClass('active');
    } else if (currentUrl.includes('saved-queries.html')) {
        $('.nav-usq').addClass('active');
    } else if (currentUrl.includes('alerts.html') || currentUrl.includes('alert.html') || currentUrl.includes('alert-details.html') || currentUrl.includes('contacts.html')) {
        $('.nav-alerts').addClass('active');
        $('.alerts-nav-tab').appendOrgNavTabs('Alerting', alertsUpperNavTabs);
    } else if (currentUrl.includes('all-slos.html')) {
        $('.nav-slos').addClass('active');
        $('.alerts-nav-tab').appendOrgNavTabs('SLOs', []);
    } else if (currentUrl.includes('cluster-stats.html') || currentUrl.includes('org-settings.html') || currentUrl.includes('application-version.html') {{ .OrgUpperNavUrls }} ) {
        $('.nav-myorg').addClass('active');
        $('.org-nav-tab').appendOrgNavTabs('My Org', orgUpperNavTabs);
    } else if (currentUrl.includes('minion-searches.html')) {
        $('.nav-minion').addClass('active');
    } else if (currentUrl.includes('live-tail.html')) {
        $('.nav-live').addClass('active');
    } else if (currentUrl.includes('service-health.html') || currentUrl.includes('service-health-overview.html') || currentUrl.includes('dependency-graph.html') || currentUrl.includes('search-traces.html')) {
        $('.nav-traces').addClass('active');
        $('.nav-traces').addClass('disable-hover');
        setTimeout(function () {
            $('.nav-traces').removeClass('disable-hover');
        }, 500);
        if ($('.subsection-navbar').length) {
            $('.subsection-navbar').appendOrgNavTabs('Tracing', tracingUpperNavTabs);
        }
    } else if (currentUrl.includes('test-data.html') || currentUrl.includes('metrics-ingestion.html') || currentUrl.includes('traces-ingestion.html')) {
        $('.nav-ingest').addClass('active');
        $('.nav-ingest').addClass('disable-hover');
        setTimeout(function () {
            $('.nav-ingest').removeClass('disable-hover');
        }, 500);
    }

    // Hover event handlers updated to respect disable-hover class
    $('.metrics-dropdown-toggle').hover(
        function () {
            if (!$(this).closest('.menu').hasClass('disable-hover')) {
                $('.metrics-dropdown').stop(true, true).slideDown(0);
            }
        },
        function () {
            if (!$(this).closest('.menu').hasClass('disable-hover')) {
                $('.metrics-dropdown').stop(true, true).slideUp(30);
            }
        }
    );

    $('.tracing-dropdown-toggle').hover(
        function () {
            if (!$(this).closest('.menu').hasClass('disable-hover')) {
                $('.traces-dropdown').stop(true, true).slideDown(0);
            }
        },
        function () {
            if (!$(this).closest('.menu').hasClass('disable-hover')) {
                $('.traces-dropdown').stop(true, true).slideUp(30);
            }
        }
    );

    $('.ingestion-dropdown-toggle').hover(
        function () {
            if (!$(this).closest('.menu').hasClass('disable-hover')) {
                $('.ingestion-dropdown').stop(true, true).slideDown(0);
            }
        },
        function () {
            if (!$(this).closest('.menu').hasClass('disable-hover')) {
                $('.ingestion-dropdown').stop(true, true).slideUp(30);
            }
        }
    );

    $('.nav-help').hover(
        function (event) {
            event.stopPropagation();
            event.preventDefault();
            $('.help-options').stop(true, true).slideDown(0);
        },
        function (event) {
            event.stopPropagation();
            event.preventDefault();
            $('.help-options').stop(true, true).slideUp(30);
        }
    );

    // Prevent the default click action for Help & Support
    $('.nav-help').on('click', function (event) {
        event.preventDefault();
    });

    // Handle the hover event for the help-options to keep it visible
    $('.help-options').hover(
        function (event) {
            event.stopPropagation();
            event.preventDefault();
            $('.help-options').stop(true, true).slideDown(0);
        },
        function (event) {
            event.stopPropagation();
            event.preventDefault();
            $('.help-options').stop(true, true).slideUp(30);
        }
    );

    $(document).on('click', function (event) {
        var helpOptions = $('.help-options');
        var metricsDropdown = $('.metrics-dropdown');
        var tracesDropdown = $('.traces-dropdown');
        var ingestionDropdown = $('.ingestion-dropdown');

        if (!metricsDropdown.is(event.target) && metricsDropdown.has(event.target).length === 0) {
            metricsDropdown.hide();
        }
        if (!tracesDropdown.is(event.target) && tracesDropdown.has(event.target).length === 0) {
            tracesDropdown.hide();
        }
        if (!ingestionDropdown.is(event.target) && ingestionDropdown.has(event.target).length === 0) {
            ingestionDropdown.hide();
        }
        if (!helpOptions.is(event.target) && helpOptions.has(event.target).length === 0) {
            helpOptions.slideUp(0);
        }
    });

    const menuItem = document.querySelectorAll('.metrics-dropdown a');
    menuItem.forEach((item) => {
        if (item.href === currentUrl) {
            item.classList.add('active');
        }
    });
});
