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
class DashboardHeader {
    constructor(containerId, options = {}) {
        this.container = $(`#${containerId}`);
        this.options = {
            title: options.title || 'Kubernetes Overview',
            activeMenuItem: options.title || 'Kubernetes Overview',
            startTime: options.startTime || 'now-1h',
            endTime: options.endTime || 'now',
            showTimeRange: options.showTimeRange !== true,
            showRefresh: options.showRefresh !== true,
            ...options,
        };
    }

    render() {
        const headerHtml = `
            <div class="d-flex align-items-center justify-content-between mb-5">
                <div class="header-left">
                    <div class="title-container">
                        <div class="title-row d-flex align-items-center">
                            <div class="kubernetes-img"></div>
                            <div class="dropdown">
                            <button class="btn p-0" type="button" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false" data-bs-toggle="dropdown">
                                    <h1 class="myOrg-heading mb-0 mx-4">${this.options.title}</h1>
                                    <img class="dropdown-arrow" src="assets/arrow-btn.svg" alt="expand" style="right:0px;">                
                            </button>
                                <div class="dropdown-menu dropdown-menu-end" aria-labelledby="" style="width: 150px;">
                                    <li class="${this.options.activeMenuItem === 'Kubernetes Overview' ? 'active' : ''}">Kubernetes Overview</li>
                                    <li class="${this.options.activeMenuItem === 'Clusters' ? 'active' : ''}">Clusters</li>
                                    <li class="${this.options.activeMenuItem === 'Namespaces' ? 'active' : ''}">Namespaces</li>
                                    <li class="${this.options.activeMenuItem === 'Workloads' ? 'active' : ''}">Workloads</li>
                                    <li class="${this.options.activeMenuItem === 'Nodes' ? 'active' : ''}">Nodes</li>
                                    <li class="${this.options.activeMenuItem === 'Events' ? 'active' : ''}">Events</li>
                                    <li class="${this.options.activeMenuItem === 'Configuration' ? 'active' : ''}">Configuration</li>
                                </div>
                            </div>                      
                        </div>
                    </div>
                </div>
                <div class="header-right d-flex gap-2 align-items-center">
                ${
                    this.options.showTimeRange
                        ? `
                    <div class="dropdown">
                        <button class="btn dropdown-toggle" type="button" id="date-picker-btn"
                            data-toggle="dropdown" aria-haspopup="true" aria-expanded="false"
                            data-bs-toggle="dropdown">
                            <span>Time Picker</span>
                            <i class="dropdown-arrow"></i>
                        </button>
                        <div class="dropdown-menu daterangepicker" aria-labelledby="index-btn"
                            id="daterangepicker ">
                            <p class="dt-header">Search the last</p>
                            <div class="ranges">
                                <div class="inner-range">
                                    <div id="now-5m" class="range-item">5 Mins</div>
                                    <div id="now-3h" class="range-item">3 Hrs</div>
                                    <div id="now-2d" class="range-item">2 Days</div>
                                </div>
                                <div class="inner-range">
                                    <div id="now-15m" class="range-item">15 Mins</div>
                                    <div id="now-6h" class="range-item">6 Hrs</div>
                                    <div id="now-7d" class="range-item">7 Days</div>
                                </div>
                                <div class="inner-range">
                                    <div id="now-30m" class="range-item">30 Mins</div>
                                    <div id="now-12h" class="range-item">12 Hrs</div>
                                    <div id="now-30d" class="range-item">30 Days</div>
                                </div>
                                <div class="inner-range">
                                    <div id="now-1h" class="range-item">1 Hr</div>
                                    <div id="now-24h" class="range-item">24 Hrs</div>
                                    <div id="now-90d" class="range-item">90 Days</div>
                                </div>
                                <hr>
                                </hr>
                                <div class="dt-header">Custom Search <span id="reset-timepicker"
                                        type="reset">Reset</span>
                                </div>
                                <div id="daterange-from"> <span id="dt-from-text"> From</span> <br />
                                    <input type="date" id="date-start" />
                                    <input type="time" id="time-start" value="00:00" />
                                </div>
                                <div id="daterange-to"> <span id="dt-to-text"> To </span> <br />
                                    <input type="date" id="date-end">
                                    <input type="time" id="time-end" value="00:00">
                                </div>
                                <div class="drp-buttons">
                                    <button class="applyBtn btn btn-sm btn-primary" id="customrange-btn"
                                        type="button">Apply</button>
                                </div>
                            </div>
                        </div>
                    </div>
                    `
                        : ''
                }
                    ${
                        this.options.showRefresh
                            ? `
                    <div class="refresh-container">
                        <button class="btn refresh-btn btn-grey" title="Refresh dashboard">
                        </button>
                        <div class="dropdown">
                            <button class="btn dropdown-toggle" type="button" id="refresh-picker-btn" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false" data-bs-toggle="dropdown">
                                <span></span>
                                <img class="orange" src="assets/arrow-btn.svg" alt="expand">                
                            </button>
                            <div class="dropdown-menu refresh-picker" aria-labelledby="index-btn" id="refresh-picker ">
                                <div class="ranges">
                                    <div id="0" class="refresh-range-item">Off</div>
                                    <div id="5m" class="refresh-range-item active">5m</div>
                                    <div id="30m" class="refresh-range-item">30m</div>
                                    <div id="1h" class="refresh-range-item">1h</div>
                                    <div id="5h" class="refresh-range-item">5h</div>
                                    <div id="1d" class="refresh-range-item">1d</div>
                                </div>
                            </div>
                        </div>
                    </div>
                    `
                            : ''
                    }
                </div>
            </div>
        `;

        this.container.html(headerHtml);
        this.attachEventListeners();
    }

    attachEventListeners() {
        this.container.find('.dropdown-menu li').on('click', function () {
            const urlParams = new URLSearchParams(window.location.search);
            const menuItem = $(this).text();
            const startTime = urlParams.get('startEpoch') || 'now-1h';
            const endTime = urlParams.get('endEpoch') || 'now';
            if (menuItem === 'Kubernetes Overview') {
                window.location.href = `kubernetes-overview.html?startEpoch=${startTime}&endEpoch=${endTime}`;
            } else {
                window.location.href = `kubernetes-view.html?type=${menuItem.toLowerCase()}&startEpoch=${startTime}&endEpoch=${endTime}`;
            }
        });

        datePickerHandler(this.options.startTime, this.options.endTime, this.options.startTime);
        setupEventHandlers();

        this.container.on('click', '.range-item', () => {
            const currentUrl = new URL(window.location.href);
            currentUrl.searchParams.set('startEpoch', filterStartDate);
            currentUrl.searchParams.set('endEpoch', filterEndDate);
            window.history.pushState({}, '', currentUrl);
        });
        
        this.container.on('dateRangeValid', '#customrange-btn', () => {
            const currentUrl = new URL(window.location.href);
            currentUrl.searchParams.set('startEpoch', filterStartDate);
            currentUrl.searchParams.set('endEpoch', filterEndDate);
            window.history.pushState({}, '', currentUrl);
        });
    }
}
