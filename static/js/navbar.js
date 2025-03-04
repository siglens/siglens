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
<div class="nav-header">
    <div class="sl-hamburger" id="navbar-toggle">
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="4" x2="20" y1="12" y2="12"></line>
            <line x1="4" x2="20" y1="6" y2="6"></line>
            <line x1="4" x2="20" y1="18" y2="18"></line>
        </svg>
        </div>
    <div class="nav-main-menu logo">
        <a href="./index.html" class="nav-links"><img class="sslogo" src="./assets/siglens-logo.svg">
        </a>
    </div>
</div>
<div class="d-flex justify-content-between h-100 flex-column">
    <div>
        <div class="menu nav-search">
            <a href="./index.html" class="nav-links"><span class="icon-search"></span><span
                    class="nav-link-text">Logs</span></a>
        </div>
        <div class="menu nav-traces tracing-dropdown-toggle"  style="display:flex;flex-direction:row">
            <a class="nav-links" href="./service-health.html">
                <span class="icon-traces"></span>
                <span class="nav-link-text">APM</span>
            </a>
            <ul class="traces-dropdown navbar-submenu">
                <a href="./service-health.html"><li class="traces-link">Service Health (RED)</li></a>
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
        <div class="menu nav-infrastructure">
            <a href="./infrastructure.html" class="nav-links"><span class="icon-infrastructure"></span><span
                    class="nav-link-text">Infrastructure</span></a>
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

const headerHTML = `
<div class="sl-header">
    <div class="sl-hamburger" id="navbar-toggle">
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="4" x2="20" y1="12" y2="12"></line>
            <line x1="4" x2="20" y1="6" y2="6"></line>
            <line x1="4" x2="20" y1="18" y2="18"></line>
        </svg>
    </div>
    <div class="nav-main-menu logo">
        <a href="./index.html" class="nav-links"><img class="sslogo" src="./assets/siglens-logo.svg">
        </a>
    </div>
    <div class="sl-breadcrumb-container">
        <ul class="sl-breadcrumb" id="sl-breadcrumb"></ul>
    </div>
</div>`;

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

const navigationStructure = {
    'index.html': {
        activeClass: 'nav-search',
        breadcrumbs: [{ name: 'Logs'}]
    },
    'metrics-explorer.html': {
        activeClass: 'nav-metrics',
        temporaryDisableHover: true,
        breadcrumbs: [{ name: 'Metrics Explorer'}]
    },
    'metric-summary.html': {
        activeClass: 'nav-metrics',
        breadcrumbs: [
            { name: 'Metrics Summary'}
        ]
    },
    'metric-cardinality.html': {
        activeClass: 'nav-metrics',
        breadcrumbs: [
            { name: 'Metrics Cardinality' }
        ]
    },
    'service-health.html': {
        activeClass: 'nav-traces',
        temporaryDisableHover: true,
        breadcrumbs: [
            { name: 'APM', noLink: true},
            { name: 'Service Health (RED)' }
        ],
        upperNavTabs: 'tracingUpperNavTabs'
    },
    'service-health-overview.html': {
        activeClass: 'nav-traces',
        breadcrumbs: [
            { name: 'APM', noLink: true},
            { name: 'Service Health' }
        ],
    },
    'search-traces.html': {
        activeClass: 'nav-traces',
        breadcrumbs: [
            { name: 'APM', url: './service-health.html', noLink: true },
            { name: 'Search Traces'}
        ],
        upperNavTabs: 'tracingUpperNavTabs'
    },
    'dependency-graph.html': {
        activeClass: 'nav-traces',
        breadcrumbs: [
            { name: 'APM', noLink: true},
            { name: 'Dependency Graph', url: './dependency-graph.html' }
        ],
        upperNavTabs: 'tracingUpperNavTabs'
    },
    'all-alerts.html': {
        activeClass: 'nav-alerts',
        breadcrumbs: [{ name: 'Alerting', url: './all-alerts.html' },
                      { name: 'Alert Rules'}],
        upperNavTabs: 'alertsUpperNavTabs'
    },
    'contacts.html': {
        activeClass: 'nav-alerts',
        breadcrumbs: [
            { name: 'Alerting', url: './all-alerts.html' },
            { name: 'Contact Points'}
        ],
        upperNavTabs: 'alertsUpperNavTabs'
    },
    'alert.html': {
        activeClass: 'nav-alerts',
    },
    'alert-details.html': {
        activeClass: 'nav-alerts',
    },
    'all-slos.html': {
        activeClass: 'nav-slos',
        breadcrumbs: [{ name: 'SLOs'}],
        upperNavTabs: 'sloTabs'
    },
    'dashboards-home.html': {
        activeClass: 'nav-ldb',
        breadcrumbs: [{ name: 'Dashboards'}]
    },
    'dashboard.html': {
        activeClass: 'nav-ldb',
    },
    'saved-queries.html': {
        activeClass: 'nav-usq',
        breadcrumbs: [{ name: 'Saved Queries'}]
    },
    'minion-searches.html': {
        activeClass: 'nav-minion',
        breadcrumbs: [{ name: 'Minion Searches'}]
    },
    'lookups.html': {
        activeClass: 'nav-lookups',
        breadcrumbs: [{ name: 'Lookups'}]
    },
    'infrastructure.html': {
        activeClass: 'nav-infrastructure',
        breadcrumbs: [{ name: 'Infrastructure'}]
    },
    'test-data.html': {
        activeClass: 'nav-ingest',
        temporaryDisableHover: true,
        breadcrumbs: [{ name: 'Log Ingestion Methods'}]
    },
    'metrics-ingestion.html': {
        activeClass: 'nav-ingest',
        breadcrumbs: [{ name: 'Metrics Ingestion Methods'}]
    },
    'traces-ingestion.html': {
        activeClass: 'nav-ingest',
        breadcrumbs: [{ name: 'Traces Ingestion Methods'}]
    }
};

// Pages related to My Org section
const orgPages = {
    'cluster-stats.html': { 
        name: 'Cluster Stats',
        breadcrumbs: [
            { name: 'My Org', noLink: true},
            { name: 'Cluster Stats' }
        ]
    }, 
    'org-settings.html': { 
        name: 'Org Settings',
        breadcrumbs: [
            { name: 'My Org', noLink: true},
            { name: 'Org Settings' }
        ]
    }, 
    'application-version.html': { 
        name: 'Version',
        breadcrumbs: [
            { name: 'My Org', noLink: true},
            { name: 'Version' }
        ]
    }, 
    'query-stats.html': { 
        name: 'Query Stats',
        breadcrumbs: [
            { name: 'My Org', noLink: true},
            { name: 'Query Stats' }
        ]
    }, 
    'pqs-settings.html': { 
        name: 'PQS Settings',
        breadcrumbs: [
            { name: 'My Org', noLink: true},
            { name: 'PQS Settings' }
        ]
    }, 
    'diagnostics.html': { 
        name: 'Diagnostics',
        breadcrumbs: [
            { name: 'My Org', noLink: true},
            { name: 'Diagnostics' }
        ]
    },
    {{ .OrgUpperNavUrls }}
};

$(document).ready(function () {
    $('#app-side-nav').before(headerHTML);
    $('#app-side-nav').prepend(navbarComponent);
    
    setupNavigationState();
    
    initializeDropdowns();
    
    setupHamburgerBehavior();
    
    $('.navbar-submenu').hide();
    $('.help-options').hide();
});

function setupNavigationState() {
    const currentUrl = window.location.href;
    let matchedConfig = null;
    let isOrgPage = false;
    
    if (currentUrl.endsWith('/') || currentUrl.endsWith('/#')) {
        matchedConfig = navigationStructure['index.html'];
    } 
    else{
        for (const [urlKey, config] of Object.entries(navigationStructure)) {
            if (currentUrl.includes(urlKey)) {
                matchedConfig = config;
                break;
            }
        }
    }
    
    // Check for org pages if no exact match
    if (!matchedConfig) {

        for (const page of Object.keys(orgPages)) {
            if (currentUrl.includes(page)) {
                isOrgPage = true;
                orgPageConfig = orgPages[page];
                break;
            }
        }        
        if (isOrgPage) {
            matchedConfig = {
                activeClass: 'nav-myorg',
                breadcrumbs: orgPageConfig.breadcrumbs,
                upperNavTabs: 'orgUpperNavTabs'
            };
        }
    }
    
    if (!matchedConfig) {
        // Check each org page
        for (const [page, config] of Object.entries(orgPages)) {
            if (currentUrl.includes(page)) {
                isOrgPage = true;
                orgPageConfig = config;
                break;
            }
        }
        
        if (isOrgPage) {
            matchedConfig = {
                activeClass: 'nav-myorg',
                breadcrumbs: orgPageConfig.breadcrumbs,
                upperNavTabs: 'orgUpperNavTabs'
            };
        }
    }
    if (matchedConfig) {
        $(`.${matchedConfig.activeClass}`).addClass('active');
        
        if (matchedConfig.temporaryDisableHover) {
            $(`.${matchedConfig.activeClass}`).addClass('disable-hover');
            setTimeout(function () {
                $(`.${matchedConfig.activeClass}`).removeClass('disable-hover');
            }, 500);
        }
        
        initializeBreadcrumbs(matchedConfig.breadcrumbs);
        
        if (matchedConfig.upperNavTabs) {
            if (matchedConfig.upperNavTabs === 'tracingUpperNavTabs' && $('.subsection-navbar').length) {
                $('.subsection-navbar').appendOrgNavTabs(tracingUpperNavTabs);
            } else if (matchedConfig.upperNavTabs === 'alertsUpperNavTabs') {
                $('.alerts-nav-tab').appendOrgNavTabs(alertsUpperNavTabs);
            } else if (matchedConfig.upperNavTabs === 'orgUpperNavTabs') {
                $('.org-nav-tab').appendOrgNavTabs(orgUpperNavTabs);
            } else if (matchedConfig.upperNavTabs === 'sloTabs') {
                $('.alerts-nav-tab').appendOrgNavTabs([]);
            }
        }
    }
}

function initializeDropdowns() {
    // Metrics dropdown behavior
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

    // Tracing dropdown behavior
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

    // Ingestion dropdown behavior
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

    // Help dropdown behavior
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
    ).on('click', function (event) {
        event.preventDefault();
    });

    // Help options hover behavior
    $('.help-options').hover(
        function (event) {
            event.stopPropagation();
            event.preventDefault();
            $(this).stop(true, true).slideDown(0);
        },
        function (event) {
            event.stopPropagation();
            event.preventDefault();
            $(this).stop(true, true).slideUp(30);
        }
    );

    // Close dropdowns when clicking elsewhere
    $(document).on('click', function (event) {
        const dropdowns = [
            { selector: '.metrics-dropdown', condition: $('.metrics-dropdown').is(event.target) || $('.metrics-dropdown').has(event.target).length > 0 },
            { selector: '.traces-dropdown', condition: $('.traces-dropdown').is(event.target) || $('.traces-dropdown').has(event.target).length > 0 },
            { selector: '.ingestion-dropdown', condition: $('.ingestion-dropdown').is(event.target) || $('.ingestion-dropdown').has(event.target).length > 0 },
            { selector: '.help-options', condition: $('.help-options').is(event.target) || $('.help-options').has(event.target).length > 0 }
        ];

        dropdowns.forEach(dropdown => {
            if (!dropdown.condition) {
                $(dropdown.selector).hide();
            }
        });
    });
    
    // Check active menu items
    const menuItem = document.querySelectorAll('.metrics-dropdown a');
    menuItem.forEach((item) => {
        if (item.href === window.location.href) {
            item.classList.add('active');
        }
    });
}

function setupHamburgerBehavior() {
    const navbarToggle = $('#navbar-toggle');
    const appSideNav = $('#app-side-nav');
    let navTimeout;

    navbarToggle.on('mouseenter', function () {
        clearTimeout(navTimeout);
        $('body').addClass('nav-expanded');
    });

    appSideNav.on('mouseenter', function () {
        clearTimeout(navTimeout);
    });

    appSideNav.on('mouseleave', function () {
        navTimeout = setTimeout(function () {
            $('body').removeClass('nav-expanded');
        }, 300);
    });

    navbarToggle.on('mouseleave', function (e) {
        if (!appSideNav.is(e.relatedTarget) && !$.contains(appSideNav[0], e.relatedTarget)) {
            navTimeout = setTimeout(function () {
                $('body').removeClass('nav-expanded');
            }, 300);
        }
    });

    navbarToggle.on('click', function (e) {
        e.stopPropagation();
        $('body').toggleClass('nav-expanded');
    });

    $(document).on('click', function (e) {
        if (!appSideNav.is(e.target) && !appSideNav.has(e.target).length &&
            !navbarToggle.is(e.target) && !navbarToggle.has(e.target).length) {
            $('body').removeClass('nav-expanded');
        }
    });
}

function initializeBreadcrumbs(breadcrumbConfig) {
    const breadcrumb = $('#sl-breadcrumb');
    breadcrumb.empty();
    
    if (breadcrumbConfig && breadcrumbConfig.length) {
        $.each(breadcrumbConfig, function(index, crumb) {
            const li = $('<li>');
            let a;
            
            if ((index === breadcrumbConfig.length - 1) || crumb.noLink) {
                // For the last item or explicitly non-clickable items, create a span instead of a link
                a = $('<span>')
                    .addClass('breadcrumb-text')
                    .text(crumb.name);
                
                if (index === breadcrumbConfig.length - 1) {
                    a.addClass('active');
                }
            } else {
                a = $('<a>')
                    .attr('href', crumb.url || '#')
                    .text(crumb.name);
            }
            
            li.append(a);
            breadcrumb.append(li);
            
            if (index < breadcrumbConfig.length - 1) {
                const arrow = $('<span>').addClass('dashboard-arrow');
                breadcrumb.append(arrow);
            }
        });
    }
}