// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
                });
            }
        });
    }
}
