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
class Breadcrumb {
    constructor(containerId, options = {}) {
        this.container = $(`#${containerId}`);
        this.options = options;
    }

    render(path) {
        const breadcrumbHtml = `
            <div id="breadcrumb">
                ${path.map((item, index) => `
                    <a href="${item.url}.html" class="breadcrumb-link">
                        <h4 class="${item.className}">${item.label}</h4>
                    </a>
                    ${index < path.length - 1 ? '<span class="dashboard-arrow"></span>' : ''}
                `).join('')}
            </div>
        `;

        this.container.html(breadcrumbHtml);
    }
}
