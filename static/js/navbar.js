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
                <span class="nav-link-text">APM</span>
            </a>
            <button class="icon-caret-down" data-dropdown="traces-dropdown"></button>
        </div>
        <div class="submenu-container">
            <div class="submenu traces-dropdown">
                <a class="nav-links" href="./service-health.html"><span class="nav-link-text">Service Health (RED)</span></a>
            </div>
            <div class="submenu traces-dropdown">
                <a class="nav-links" href="./search-traces.html"><span class="nav-link-text">Search Traces</span></a>
            </div>
            <div class="submenu traces-dropdown">
                <a class="nav-links" href="./dependency-graph.html"><span class="nav-link-text">Dependency Graph</span></a>
            </div>
        </div>
        <div class="menu nav-metrics metrics-dropdown-toggle"  style="display:flex;flex-direction:row">
            <a class="nav-links" href="./metrics-explorer.html">
                <span class="icon-metrics"></span>
                <span class="nav-link-text">Metrics</span>
            </a>
            <button class="icon-caret-down" data-dropdown="metrics-dropdown"></button>
        </div>
        <div class="submenu-container">
            <div class="submenu metrics-dropdown">
                <a class="nav-links" href="./metrics-explorer.html"><span class="nav-link-text">Explorer</span></a>
            </div>
            <div class="submenu metrics-dropdown">
                <a class="nav-links" href="./metric-summary.html"><span class="nav-link-text">Summary</span></a>
            </div>
            <div class="submenu metrics-dropdown">
                <a class="nav-links" href="./metric-cardinality.html"><span class="nav-link-text">Cardinality</span></a>
            </div>
        </div>
        {{ if .ShowSLO }}        
        <div class="menu nav-slos">
            <a href="./all-slos.html" class="nav-links"><span class="icon-live"></span><span
                    class="nav-link-text">SLOs</span></a>
        </div>
        {{ end }}
        <div class="menu nav-alerts">
            <a href="./all-alerts.html" class="nav-links"><span class="icon-alerts"></span><span class="nav-link-text">Alerting</span></a>
        </div>
        <div class="menu nav-ldb">
            <a href="../dashboards-home.html" class="nav-links">
                <span class="icon-launchdb"></span><span class="nav-link-text">Dashboards</span></a>
        </div>
        {{ if not .EnterpriseEnabled }}
        <div class="menu nav-minion">
            <a href="./minion-searches.html" class="nav-links"><span class="icon-minion"></span><span
                    class="nav-link-text">Minion</span></a>
        </div>
        {{ end }}
        <div class="menu nav-usq">
            <a href="./saved-queries.html" class="nav-links"><span class="icon-usq"></span><span
                    class="nav-link-text">Saved Queries</span></a>
        </div>
        <div class="menu nav-myorg">
            <a href="./cluster-stats.html" class="nav-links"><span class="icon-myorg"></span><span
                    class="nav-link-text">My Org</span></a>
        </div>
        <div class="menu nav-lookups">
            <a href="./lookups.html" class="nav-links"><span class="icon-search"></span><span
                    class="nav-link-text">Lookups</span></a>
        </div>
        <div class="menu nav-ingest ingestion-dropdown-toggle"  style="display:flex;flex-direction:row">
            <a class="nav-links" href="./test-data.html">
                <span class="icon-ingest"></span>
                <span class="nav-link-text">Ingestion</span>
            </a>
            <button class="icon-caret-down" data-dropdown="ingestion-dropdown"></button>
        </div>
        <div class="submenu-container">
            <div class="submenu ingestion-dropdown">
                <a class="nav-links" href="./test-data.html"><span class="nav-link-text">Log Ingestion</span></a>
            </div>
            <div class="submenu ingestion-dropdown">
                <a class="nav-links" href="./metrics-ingestion.html"><span class="nav-link-text">Metrics Ingestion</span></a>
            </div>
        <div class="submenu ingestion-dropdown">
                <a class="nav-links" href="./traces-ingestion.html"><span class="nav-link-text">Traces Ingestion</span></a>                
            </div>
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
    { name: 'Org Settings', url: './org-settings.html', class: 'org-settings' },
    { name: 'PQS', url: './pqs-settings.html', class: 'pqs-settings' },
    { name: 'Query Stats', url: './query-stats.html', class: 'query-stats' },
    { name: 'Version', url: './application-version.html', class: 'application-version' },
    { name: 'Diagnostics', url: './diagnostics.html', class: 'diagnostics' },
];

let tracingUpperNavTabs = [
    { name: 'Service Health (RED)', url: './service-health.html', class: 'service-health' },
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
        highlightContent('nav-search');
    } else if (currentUrl.includes('metrics-explorer.html')) {
        highlightContent('nav-metrics');
        $('.nav-metrics .icon-caret-down').addClass('up');
        $('.nav-metrics').addClass('disable-hover');
        setTimeout(function () {
            $('.nav-metrics').removeClass('disable-hover');
        }, 500);
        $('.metrics-dropdown').show();
    } else if (currentUrl.includes('metric-summary.html')) {
        highlightContent('nav-metrics');
        $('.nav-metrics .icon-caret-down').addClass('up');
        $('.metrics-dropdown').show();
    } else if (currentUrl.includes('metric-cardinality.html')) {
        highlightContent('nav-metrics');
        $('.nav-metrics .icon-caret-down').addClass('up');
        $('.metrics-dropdown').show();
    } else if (currentUrl.includes('dashboards-home.html') || currentUrl.includes('dashboard.html')) {
        highlightContent('nav-ldb');
    } else if (currentUrl.includes('saved-queries.html')) {
        highlightContent('nav-usq');
    } else if (currentUrl.includes('alerts.html') || currentUrl.includes('alert.html') || currentUrl.includes('alert-details.html') || currentUrl.includes('contacts.html')) {
        highlightContent('nav-alerts');
        $('.alerts-nav-tab').appendOrgNavTabs('Alerting', alertsUpperNavTabs);
    } else if (currentUrl.includes('all-slos.html')) {
        highlightContent('nav-slos');
        $('.alerts-nav-tab').appendOrgNavTabs('SLOs', []);
    } else if (currentUrl.includes('cluster-stats.html') || currentUrl.includes('org-settings.html') || currentUrl.includes('application-version.html')|| currentUrl.includes('query-stats.html')  || currentUrl.includes('pqs-settings.html') {{ .OrgUpperNavUrls }}
    ||  currentUrl.includes('diagnostics.html')) {
        highlightContent('nav-myorg');
        $('.org-nav-tab').appendOrgNavTabs('My Org', orgUpperNavTabs);
    } else if (currentUrl.includes('minion-searches.html')) {
        highlightContent('nav-minion');
    } else if (currentUrl.includes('live-tail.html')) {
        highlightContent('nav-live');
    } else if (currentUrl.includes('service-health.html') || currentUrl.includes('service-health-overview.html') || currentUrl.includes('dependency-graph.html') || currentUrl.includes('search-traces.html')) {
        highlightContent('nav-traces');
        $('.nav-traces .icon-caret-down').addClass('up');
        $('.traces-dropdown').show();
        $('.nav-traces').addClass('disable-hover');
        setTimeout(function () {
            $('.nav-traces').removeClass('disable-hover');
        }, 500);
        if ($('.subsection-navbar').length) {
            $('.subsection-navbar').appendOrgNavTabs('APM', tracingUpperNavTabs);
        }
    } else if (currentUrl.includes('test-data.html') || currentUrl.includes('metrics-ingestion.html') || currentUrl.includes('traces-ingestion.html')) {
        $('.nav-ingest .icon-caret-down').addClass('up');
        highlightContent('nav-ingest');
        $('.ingestion-dropdown').show();
        $('.nav-ingest').addClass('disable-hover');
        setTimeout(function () {
            $('.nav-ingest').removeClass('disable-hover');
        }, 500);
    } else if (currentUrl.includes('lookups.html')) {
        highlightContent('nav-lookups');
    } 

    $('.icon-caret-down').on('click', function(e) {
        e.stopPropagation();
        const dropdownClass = $(this).data('dropdown');
        const $dropdown = $('.' + dropdownClass);
        const $allDropdowns = $('.submenu');
        $allDropdowns.not($dropdown).slideUp(300);
        $('.icon-caret-down').not(this).removeClass('up');
        $dropdown.slideToggle(300);
        $(this).toggleClass('up');
    });


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
        if (!helpOptions.is(event.target) && helpOptions.has(event.target).length === 0) {
            helpOptions.slideUp(0);
        }
    });

    const setActiveMenuItem = (selector) => {
        document.querySelectorAll(selector).forEach((item) => {
            if (item.href === currentUrl) {
                item.classList.add('active');
            }
        });
    };
    
    ['.metrics-dropdown a', '.traces-dropdown a', '.ingestion-dropdown a'].forEach(setActiveMenuItem);
    
    function highlightContent(navClass) {
        $(`.${navClass}`).addClass('active');
        $(`.${navClass} .icon-caret-down`).addClass('active');
    }
});
