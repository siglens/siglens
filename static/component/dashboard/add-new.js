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
        this.selectedFolderId = this.getCurrentFolderId();
        this.init();
    }

    init() {
        const dashboardModal = `
        <div class="popupContent" id="new-dashboard-modal">
            <h3 class="header">New Dashboard</h3>
            <div>
                <input type="text" placeholder="Name" class="input" id="db-name">
                <p class="error-tip"></p>
                <input type="text" placeholder="Description (Optional)" class="input mt-3" id="db-description">
                <div class="mt-3">
                    <label>Folder</label>
                    <div id="dashboard-folder-selector"></div>
                </div>
            </div>
            <div id="buttons-popupContent">
                <button type="button" id="cancel-dbbtn" class="btn btn-secondary">Cancel</button>
                <button type="button" id="save-dbbtn" class="btn btn-primary">Save</button>
            </div>
        </div>
    `;

        this.container.html(`
            <div class="dropdown">
                <button class="btn dropdown-toggle btn-primary" data-toggle="dropdown" 
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

            ${dashboardModal}

            <!-- Folder Modal -->
            <div class="popupContent" id="new-folder-modal">
                <h3 class="header">New Folder</h3>
                <div>
                    <input type="text" placeholder="Name" class="input" id="folder-name">
                    <p class="error-tip"></p>
                </div>
                <div id="buttons-popupContent">
                    <button type="button" id="cancel-dbbtn" class="btn btn-secondary">Cancel</button>
                    <button type="button" id="save-folder-btn" class="btn btn-primary">Save</button>
                </div>
            </div>
        `);

        this.setupEventHandlers();
    }

    setupEventHandlers() {
        $('#create-db-btn').on('click', () => this.showDashboardModal());

        $('#create-folder-btn').on('click', () => this.showFolderModal());

        $('.popupOverlay, #cancel-dbbtn').on('click', () => this.closeModals());

        $('#save-dbbtn').on('click', () => this.createDashboard());
        $('#save-folder-btn').on('click', () => this.createFolder());

        $('#db-name, #folder-name').on('focus', function () {
            $('.error-tip').removeClass('active').text('');
        });

        $(document).on('keydown', (event) => {
            if (event.key === 'Escape') {
                this.closeModals();
            }
        });

        $('#db-name, #db-description').on('keydown', (event) => {
            if (event.key === 'Enter') {
                this.createDashboard();
            }
        });

        $('#folder-name').on('keydown', (event) => {
            if (event.key === 'Enter') {
                this.createFolder();
            }
        });
    }

    showDashboardModal() {
        $('.popupOverlay, #new-dashboard-modal').addClass('active');
        $('.error-tip').removeClass('active');
        $('#db-name').focus();

        const currentFolderId = this.getCurrentFolderId();
        const currentFolderName = currentFolderId === 'root-folder' ? 'Dashboards' : $('.name-dashboard').text().trim();

        this.folderDropdown = new FolderDropdown('dashboard-folder-selector', {
            placeholder: 'Select Folder',
            showRoot: true,
            initialFolder: {
                id: currentFolderId,
                name: currentFolderName,
            },
            onSelect: (folder) => {
                this.selectedFolderId = folder.id;
            },
        });
    }
    showFolderModal() {
        $('.popupOverlay, #new-folder-modal').addClass('active');
        $('.error-tip').removeClass('active');
        $('#folder-name').focus();
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
            parentId: this.selectedFolderId,
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
            window.location.href = `folder.html?id=${response.id}`;
        } catch (error) {
            $('#new-folder-modal .error-tip').text(`${error.responseJSON.message}`).addClass('active');
        }
    }

    getCurrentFolderId() {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get('id') || 'root-folder';
    }
}
