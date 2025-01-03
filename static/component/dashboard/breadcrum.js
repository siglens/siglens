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
    constructor(containerId = 'breadcrumb') {
        this.container = $(`#${containerId}`);
    }

    render(breadcrumbs = [], currentName = '', showFavorite = false, isFavorite = false) {
        // Filter out root and current folder
        const filteredBreadcrumbs =
            breadcrumbs?.filter(
                (crumb) => crumb.id !== 'root-folder' && crumb.name !== 'Root' && crumb.name !== currentName // Filter out current folder name
            ) || [];

        this.container.empty().append(`
            <a href="../dashboards-home.html" class="breadcrumb-link">
                <h4 class="all-dashboards">All Dashboards</h4>
            </a>
            <span class="dashboard-arrow"></span>
            ${filteredBreadcrumbs
                .map(
                    (crumb) => `
                <a href="../folder-html?id=${crumb.id}" class="breadcrumb-link">
                    <h4 class="folder-name">${crumb.name}</h4>
                </a>
                <span class="dashboard-arrow"></span>
            `
                )
                .join('')}
            <h3 class="name-dashboard">${currentName}</h3>
            ${
                showFavorite
                    ? `
            <button class="star-icon ${isFavorite ? 'favorited' : ''}" 
                    id="favbutton" 
                    title="Mark as favorite">
            </button>
        `
                    : ''
            }
        `);
    }

    onFavoriteClick(callback) {
        this.container.on('click', '#favbutton', (e) => {
            callback(e);
        });
    }
}
