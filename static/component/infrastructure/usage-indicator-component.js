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
class ResourceUsageComponent {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.init();
    }

    init() {
        this.render();
        this.initTooltips();
    }

    render() {
        const html = `
        <div class="usage-indicators">
            <span>Resource usage:</span>
            <div class="indicator low" id="low-usage">
                <div class="bucket-icon"></div>
                low
            </div>
            <div class="indicator med" id="med-usage">
                <div class="bucket-icon"></div>
                med
            </div>
            <div class="indicator high" id="high-usage">
                <div class="bucket-icon"></div>
                high
            </div>
            <div class="indicator unknown" id="unknown-usage">
                <i class="fa fa-question-circle"></i>
                unknown
            </div>
        </div>
        `;

        if (this.container) {
            this.container.innerHTML = html;
        }
    }

    initTooltips() {
        const tooltipConfig = {
            low: 'Usage is under 60% (underutilized)',
            med: 'Usage is between 60% and 90% (well utilized)',
            high: 'Usage is over 90% (overutilized)',
            unknown: 'Requests not set',
        };

        // Initialize tippy tooltips
        Object.entries(tooltipConfig).forEach(([level, content]) => {
            const element = document.getElementById(`${level}-usage`);
            if (element) {
                tippy(element, {
                    content,
                    placement: 'bottom',
                    arrow: true,
                    theme: 'custom',
                    animation: 'scale',
                    delay: [100, 0],
                });
            }
        });
    }
}
