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
    constructor(containerId = 'sl-breadcrumb') {
        this.container = $(`#${containerId}`);
    }

    render(breadcrumbs = [], currentName = '', showFavorite = false, isFavorite = false) {
        this.container.empty();

        // Filter out root and current folder
        const filteredBreadcrumbs = breadcrumbs?.filter((crumb) => crumb.id !== 'root-folder' && crumb.name !== 'Root' && crumb.name !== currentName) || [];

        // Add "All Dashboards" item
        this.addBreadcrumbItem('All Dashboards', '../dashboards-home.html');

        // Add separator after "All Dashboards"
        if (filteredBreadcrumbs.length > 0 || currentName) {
            this.addSeparator();
        }

        // Add folder breadcrumbs
        filteredBreadcrumbs.forEach((crumb, index) => {
            this.addBreadcrumbItem(crumb.name, `../folder.html?id=${crumb.id}`);

            // Add separator if not the last item or if there's a current name
            if (index < filteredBreadcrumbs.length - 1 || currentName) {
                this.addSeparator();
            }
        });

        // Add current dashboard name (active)
        if (currentName) {
            this.addNonClickableItem(currentName);
        }

        if (showFavorite) {
            const favButton = $('<button>')
                .addClass('star-icon' + (isFavorite ? ' favorited' : ''))
                .attr('id', 'favbutton')
                .attr('title', 'Mark as favorite');

            $('.sl-breadcrumb-container').append(favButton);
        }
    }

    addBreadcrumbItem(text, url, isActive = false) {
        const li = $('<li>');
        const a = $('<a>').attr('href', url).text(text);

        if (isActive) {
            a.addClass('active');
        }

        li.append(a);
        this.container.append(li);
    }

    addNonClickableItem(text) {
        const li = $('<li>');
        const span = $('<span>').addClass('breadcrumb-text active name-dashboard').text(text);

        li.append(span);
        this.container.append(li);
    }

    addSeparator() {
        const separator = $('<span>').addClass('dashboard-arrow');
        this.container.append(separator);
    }

    onFavoriteClick(callback) {
        $('.sl-breadcrumb-container').on('click', '#favbutton', callback);
    }
}
