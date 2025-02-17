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
        <div class="menu nav-metrics">  
            <a class="nav-links accordion-toggle" href="./metrics.html" onclick="toggleDropdown(this)">  
                <div class="nav-link-content">  
                    <span class="icon-metrics"></span>  
                    <span class="nav-link-text">Metrics</span>  
                    <span class="dropdown-arrow"></span>
                </div>  
            </a>  
            <div class="accordion-content" style="display: none;">  
                <a href="./metrics-explorer.html" class="submenu-link">  
                    <span class="nav-link-text">Explorer</span>  
                </a>  
                <a href="./metric-summary.html" class="submenu-link">  
                    <span class="nav-link-text">Summary</span>  
                </a>  
                <a href="./metric-cardinality.html" class="submenu-link">  
                    <span class="nav-link-text">Cardinality</span>  
                </a>  
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


const accordionStyles = `
<style>  
    .accordion-toggle {  
        border: none;  
        background: none;  
        cursor: pointer;  
        padding: 0 !important;  
        display: flex;  
        justify-content: space-between;  
        align-items: center;  
    }  

    .nav-link-content {  
        display: flex;  
        align-items: center;  
    }  

    .accordion-content {  
        overflow: hidden;  
        transition: max-height 0.5s ease-in-out, opacity 0.3s ease-in-out, padding 0.3s ease-in-out;  
        margin-left: 16px; 
        padding: 0;  
        gap: 3px;  
        display: none; 
    }  

    .submenu-link {  
        display: block;  
        padding: 8px 15px 8px 30px; 
        text-decoration: none;  
        color: inherit;  
        position: relative;  
    }  

    .submenu-link:hover {  
        background-color: rgba(0, 0, 0, 0.05);  
    }  

    /* Vertical line for submenu links */  
    .submenu-link::before {  
        content: '';  
        position: absolute;  
        left: 0;  
        top: 50%;  
        width: 1px; 
        height: 100%;  
        background-color: grey; 
        transform: translateY(-50%);  
        opacity: 0;  
        transition: opacity 0.3s ease;  
    }  

    /* Show line when dropdown is open */  
    .accordion-content.active .submenu-link::before {  
        opacity: 1; 
    }  
    
     .submenu-link::before {  
        opacity: 1;
    } 

     /* on hover */
    .submenu-link:hover::before {  
        opacity: 1;
        background-color: orange;  
        width: 2px;
    }  
 
    .dropdown-arrow {  
        transition: transform 0.3s ease;  
        display: inline-block; 
    }  

    .dropdown-arrow.active {  
        transform: rotate(90deg);   
    }  

    .accordion-content.active {  
        display: block;  
    }  
</style> 
`;

// Add the styles to the head
$('head').append(accordionStyles);


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

    const isNavigatingBack = document.referrer.includes('metrics.html');
    //Handling Dropdown
    $('.nav-header').on('click', function(e) {
        e.preventDefault();
        const $menu = $(this).closest('.menu');
        const $submenu = $menu.find('.submenu');
        const $arrow = $menu.find('.dropdown-arrow');
        
        // Close all other submenus
        $('.submenu').not($submenu).slideUp(300);
        $('.dropdown-arrow').not($arrow).removeClass('active');
        
        // Toggle current submenu
        $submenu.slideToggle(300);
        $arrow.toggleClass('active');
        
        // Handle active states
        if ($submenu.is(':visible')) {
            $menu.addClass('active');
        } else {
            $menu.removeClass('active');
        }
    });


    if (currentUrl.includes('index.html')) {
        $('.nav-search').addClass('active');
    }
    if(currentUrl.includes('metrics.html')){
        $('.nav-metrics').addClass('active');
        $('.nav-metrics .accordion-content').show();
        $('.nav-metrics .dropdown-arrow').addClass('active');
    }
    if (isNavigatingBack) {
        $('.nav-metrics .accordion-content').hide();
        $('.nav-metrics').removeClass('active');
        $('.nav-metrics .dropdown-arrow').removeClass('active');
    } 
    if (currentUrl.includes('metrics-explorer.html') || 
        currentUrl.includes('metric-summary.html') || 
        currentUrl.includes('metric-cardinality.html')) {
        $('.nav-metrics').addClass('active');
        $('.nav-metrics .accordion-content').show();
        $('.nav-metrics .dropdown-arrow').addClass('active');
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
    } else if (currentUrl.includes('cluster-stats.html') || currentUrl.includes('org-settings.html') || currentUrl.includes('application-version.html')|| currentUrl.includes('query-stats.html')  || currentUrl.includes('pqs-settings.html') {{ .OrgUpperNavUrls }}
    ||  currentUrl.includes('diagnostics.html')) {
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
    } else if (currentUrl.includes('lookups.html')) {
        $('.nav-lookups').addClass('active');
    } 

    // // Hover event handlers updated to respect disable-hover class
    // $('.metrics-dropdown-toggle').hover(
    //     function () {
    //         if (!$(this).closest('.menu').hasClass('disable-hover')) {
    //             $('.metrics-dropdown').stop(true, true).slideDown(0);
    //         }
    //     },
    //     function () {
    //         if (!$(this).closest('.menu').hasClass('disable-hover')) {
    //             $('.metrics-dropdown').stop(true, true).slideUp(30);
    //         }
    //     }
    // );

    $('.nav-metrics .accordion-toggle').on('click', function(e) {  
        e.preventDefault();  
        const $menu = $(this).closest('.menu');  
        const $content = $menu.find('.accordion-content');  
        const $arrow = $menu.find('.dropdown-arrow');  
    
        if ($(e.target).closest('.nav-link-content').length) {  
            if (!currentUrl.includes('metrics.html')) {  
                window.location.href = './metrics.html';  
            }  
            
            // Toggle dropdown and manage active state  
            $content.slideToggle(300, function () {  
                $arrow.toggleClass('active');  
                if($content.is(':visible')) {  
                    $menu.addClass('active');  
                    $content.addClass('active'); // Add active class to accordion content  
                } else {  
                    $menu.removeClass('active');  
                    $content.removeClass('active'); // Remove active class when closed  
                }  
            });  
        } else {  
            $content.slideToggle(300);  
            $arrow.toggleClass('active');  
            $menu.toggleClass('active');  
        }  
    });

    // Handle browser back button
    window.onpopstate = function(event) {
        if (!window.location.href.includes('metrics.html')) {
            $('.nav-metrics .accordion-content').slideUp(300);
            $('.nav-metrics').removeClass('active');
            $('.nav-metrics .dropdown-arrow').removeClass('active');
        }
    };

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
