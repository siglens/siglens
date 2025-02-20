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
class BigNumberCard {
    constructor(containerId, header, number, showAll = true) {
        this.container = $(`#${containerId}`);
        this.number = number;
        this.header = header;
        this.showAll = showAll;
        this.type = containerId;
        this.init();
        this.fetchData();
    }

    getQueryForType() {
        const urlParams = new URLSearchParams(window.location.search);
        const clusterFilter = urlParams.get('cluster') || 'all';
        const namespaceFilter = urlParams.get('namespace') || 'all';

        const clusterMatch = clusterFilter === 'all' ? '.+' : clusterFilter;
        const namespaceMatch = namespaceFilter === 'all' ? '.+' : namespaceFilter;

        const queries = {
            clusters: `count(count by (cluster) (kube_pod_info{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}"}))`,
            nodes: `count(group by (cluster, node) (kube_pod_info{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}", node!=""}))`,
            namespaces: `count(group by (cluster, namespace) (kube_namespace_status_phase{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}"}))`,
            workloads: `count(group by (cluster, namespace, workload, workload_type) (namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}", workload!=""}))`,
            pods: `count(group by (cluster, namespace, pod) (kube_pod_info{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}", pod!=""}))`,
            containers: `count(group by (cluster, namespace, pod, container) (kube_pod_container_info{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}", pod!="", container!=""}))`,
        };
        return queries[this.type] || '';
    }

    async fetchData() {
        const urlParams = new URLSearchParams(window.location.search);
        const startTime = urlParams.get('startEpoch') || 'now-1h';
        const endTime = urlParams.get('endEpoch') || 'now';

        const query = this.getQueryForType();
        if (!query) return;

        const requestData = {
            start: startTime,
            end: endTime,
            queries: [
                {
                    name: 'a',
                    query: query,
                    qlType: 'promql',
                    state: 'raw',
                },
            ],
            formulas: [
                {
                    formula: 'a',
                },
            ],
        };

        try {
            const response = await this.fetchTimeSeriesData(requestData);
            this.updateValue(response);
        } catch (error) {
            this.showError();
        }
    }

    async fetchTimeSeriesData(data) {
        return await $.ajax({
            method: 'post',
            url: 'metrics-explorer/api/v1/timeseries',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(data),
        });
    }

    updateValue(response) {
        const contentDiv = this.container.find('.cluster-content');

        if (!response || !response.values || response.values.length === 0) {
            contentDiv.html('<div class="big-number error">No data</div>');
            return;
        }

        // Sum up
        const total = response.series.length;

        contentDiv.html(`<div class="big-number">${total}</div>`);
    }

    showError() {
        const contentDiv = this.container.find('.cluster-content');
        contentDiv.html('<div class="big-number error">Error</div>');
    }

    init() {
        const template = `
            <div class="cluster-card">
                <div class="cluster-header">
                    <span class="header-title">${this.header}</span>
                    <div class="header-actions">
                        ${this.showAll ? '<span class="header-link">All</span>' : ''}
                        <div class="dropdown">
                            <button class="menu-button btn"></button>
                            <div class="dropdown-content">
                                <li class="explore-option">
                                    <span class="explore-icon"></span>
                                    Explore
                                </li>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="cluster-content">
                    <div class="big-number">Loading...</div>
                </div>
            </div>
        `;

        this.container.html(template);
        this.setupEventHandlers();
    }

    setupEventHandlers() {
        const urlParams = new URLSearchParams(window.location.search);
        const startTime = urlParams.get('startEpoch') || 'now-1h';
        const endTime = urlParams.get('endEpoch') || 'now';

        this.container.find('.header-link').on('click', (e) => {
            e.preventDefault();
            window.location.href = `kubernetes-view.html?type=${this.type}&startEpoch=${startTime}&endEpoch=${endTime}`;
        });

        const dropdown = this.container.find('.dropdown');
        const menuButton = dropdown.find('.menu-button');

        menuButton.on('click', (e) => {
            e.stopPropagation();
            const currentDropdown = $(e.currentTarget).closest('.dropdown');
            $('.dropdown').not(currentDropdown).removeClass('active');
            currentDropdown.toggleClass('active');
        });

        $(document).on('click', () => {
            $('.dropdown').removeClass('active');
        });

        this.container.find('.explore-option').on('click', (e) => {
            e.stopPropagation();
        });
    }
}
