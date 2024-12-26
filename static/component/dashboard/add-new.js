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

class AddNewComponent {
    constructor(containerId) {
        this.container = $(`#${containerId}`);
        this.init();
    }

    init() {
        // Add dropdown button
        this.container.html(`
            <div class="dropdown">
                <button class="btn dropdown-toggle primary-btn" data-toggle="dropdown" 
                        aria-haspopup="true" aria-expanded="true" data-bs-toggle="dropdown">
                    <span>
                        <img src="./assets/add-icon.svg" class="add-icon">
                    </span>New
                </button>
                <div class="dropdown-menu box-shadow dropdown-menu-style dd-width-150">
                    <li class="dropdown-option" id="create-db-btn">New Dashboard</li>
                    <li class="dropdown-option" id="create-folder-btn">New Folder</li>
                </div>
            </div>

            <!-- Dashboard Modal -->
            <div class="popupContent" id="new-dashboard-modal">
                <h3 class="header">New Dashboard</h3>
                <div>
                    <input type="text" placeholder="Name" class="input" id="db-name">
                    <p class="error-tip"></p>
                    <input type="text" placeholder="Description (Optional)" class="input mt-3" id="db-description">
                </div>
                <div id="buttons-popupContent">
                    <button type="button" id="cancel-dbbtn">Cancel</button>
                    <button type="button" id="save-dbbtn">Save</button>
                </div>
            </div>

            <!-- Folder Modal -->
            <div class="popupContent" id="new-folder-modal">
                <h3 class="header">New Folder</h3>
                <div>
                    <input type="text" placeholder="Name" class="input" id="folder-name">
                    <p class="error-tip"></p>
                </div>
                <div id="buttons-popupContent">
                    <button type="button" id="cancel-dbbtn">Cancel</button>
                    <button type="button" id="save-folder-btn" class="primary-btn">Save</button>
                </div>
            </div>
        `);

        this.setupEventHandlers();
    }

    setupEventHandlers() {
        // New Dashboard button
        $('#create-db-btn').on('click', () => this.showDashboardModal());

        // New Folder button
        $('#create-folder-btn').on('click', () => this.showFolderModal());

        // Cancel buttons and overlay
        $('.popupOverlay, #cancel-dbbtn').on('click', () => this.closeModals());

        // Save handlers
        $('#save-dbbtn').on('click', () => this.createDashboard());
        $('#save-folder-btn').on('click', () => this.createFolder());

        $('#db-name, #folder-name').on('focus', function () {
            $('.error-tip').removeClass('active').text(''); // Clear error on focus
        });
    }

    showDashboardModal() {
        $('.popupOverlay, #new-dashboard-modal').addClass('active');
        $('.error-tip').removeClass('active');
    }

    showFolderModal() {
        $('.popupOverlay, #new-folder-modal').addClass('active');
        $('.error-tip').removeClass('active');
    }

    closeModals() {
        $('.popupOverlay, .popupContent').removeClass('active');
        $('#db-name, #folder-name, #db-description').val('');
        $('.error-tip').removeClass('active');
    }

    async createDashboard() {
        const dashboardData = {
            name: $('#db-name').val().trim(),
            description: $('#db-description').val().trim(),
            parentId: this.getCurrentFolderId(),
        };

        if (!dashboardData.name) {
            $('#new-dashboard-modal .error-tip').addClass('active').text('Dashboard name is required!');
            return;
        }

        try {
            const response = await $.ajax({
                method: 'post',
                url: 'api/dashboards/create',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                data: JSON.stringify(dashboardData),
            });

            this.closeModals();
            window.location.href = `dashboard.html?id=${Object.keys(response)[0]}`;
        } catch (error) {
            if (error.status === 409) {
                $('#new-dashboard-modal .error-tip').text('Dashboard name already exists in this folder!').addClass('active');
            }
        }
    }

    async createFolder() {
        const folderName = $('#folder-name').val().trim();

        if (!folderName) {
            $('#new-folder-modal .error-tip').addClass('active').text('Folder name is required!');
            return;
        }

        try {
            const response = await $.ajax({
                method: 'post',
                url: 'api/dashboards/folders/create',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                data: JSON.stringify({
                    name: folderName,
                    parentId: this.getCurrentFolderId(),
                }),
            });

            this.closeModals();
            // Refresh the current view
            // window.location.reload();
            window.location.href = `folder.html?id=${response.id}`;
        } catch (error) {
            console.log(error);
            $('#new-folder-modal .error-tip').text(`${error.responseJSON.message}`).addClass('active');
        }
    }

    getCurrentFolderId() {
        // Get current folder ID from URL or default to root
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get('id') || 'root-folder';
    }
}
