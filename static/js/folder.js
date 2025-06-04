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

let folderId;
let currentFolderParentId = null;
let currentFolderContents = null;

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);

    folderId = getCurrentFolderId();

    const { loadData } = initializeDashboardPage();
    await loadData();

    currentFolderContents = await getFolderContents(folderId);
    await loadFolderContents(currentFolderContents);

    $('#move-folder').click(showMoveModal);
    $('#delete-folder').click(showDeleteModal);
});

async function loadFolderContents(response) {
    // Find parent from breadcrumbs (second to last item)
    const breadcrumbs = response.breadcrumbs || [];
    const parentBreadcrumb = breadcrumbs[breadcrumbs.length - 2];
    currentFolderParentId = parentBreadcrumb ? parentBreadcrumb.id : 'root-folder';

    const breadcrumb = new Breadcrumb();
    breadcrumb.render(response.breadcrumbs, response.folder.name, false, false, false, true);

    initializeFolderNameEditor(response.folder.name, response.folder.id);
}

async function showMoveModal() {
    const countData = await getFolderCount(folderId);
    const message = getCountMessage(countData.total + 1, countData.folders + 1, countData.dashboards);
    $('.content-count').text(message);

    $('.popupOverlay, #move-folder-modal').addClass('active');

    const folderDropdown = new FolderDropdown('folder-selector', {
        placeholder: 'Select Folder',
        excludeFolderId: folderId,
        showRoot: true,
        onSelect: () => {
            $('.move-btn').prop('disabled', false);
        },
    });

    $('.move-btn')
        .prop('disabled', true)
        .off('click')
        .on('click', async function () {
            try {
                const selectedFolder = folderDropdown.getSelectedFolder();
                if (selectedFolder) {
                    await moveFolder(folderId, selectedFolder.id === 'root-folder' ? 'root-folder' : selectedFolder.id);
                    $('.popupOverlay, #move-folder-modal').removeClass('active');
                    window.location.reload();
                }
            } catch (error) {
                showToast('Failed to move folder. Please try again.', 'error');
            }
        });

    $('.cancel-btn').click(function () {
        $('.popupOverlay, .popupContent').removeClass('active');
    });
}

async function showDeleteModal() {
    const countData = await getFolderCount(folderId);
    const message = getCountMessage(countData.total + 1, countData.folders + 1, countData.dashboards);
    $('.content-count').text(message);

    $('.popupOverlay, #delete-folder-modal').addClass('active');

    $('.confirm-input').on('input', function () {
        $('.delete-btn').prop('disabled', $(this).val() !== 'Delete');
    });

    $('.delete-btn').click(function () {
        if ($('.confirm-input').val() === 'Delete') {
            deleteFolder(folderId).then(() => {
                $('.popupOverlay, #delete-folder-modal').removeClass('active');

                const currentFolderId = new URLSearchParams(window.location.search).get('id');
                if (currentFolderId === folderId) {
                    if (currentFolderParentId && currentFolderParentId !== 'root-folder') {
                        window.location.href = `folder.html?id=${currentFolderParentId}`;
                    } else {
                        window.location.href = 'dashboards-home.html';
                    }
                } else {
                    window.location.reload();
                }
            });
        }
    });

    $('.cancel-btn').click(function () {
        $('.popupOverlay, .popupContent').removeClass('active');
        $('.confirm-input').val('');
    });
}

function moveFolder(folderId, newParentId) {
    return $.ajax({
        method: 'put',
        url: `api/dashboards/folders/${folderId}`,
        headers: {
            'Content-Type': 'application/json',
            Accept: '*/*',
        },
        data: JSON.stringify({
            parentId: newParentId,
        }),
    }).catch((error) => {
        throw error;
    });
}

class FolderNameEditor {
    constructor(containerId, folderId) {
        this.container = document.getElementById(containerId);
        this.folderId = folderId;
        this.display = this.container.querySelector('.folder-name-display');
        this.input = this.container.querySelector('.folder-name-input');
        this.editIcon = this.container.querySelector('.folder-edit-icon');

        this.isEditing = false;
        this.originalValue = '';

        this.init();
    }

    init() {
        this.editIcon.addEventListener('click', () => this.enterEditMode());
        this.input.addEventListener('keydown', (e) => this.handleKeydown(e));
        this.input.addEventListener('blur', () => this.saveChanges());
        this.input.addEventListener('click', (e) => e.stopPropagation());
    }

    enterEditMode() {
        if (this.isEditing) return;

        this.isEditing = true;
        this.originalValue = this.display.textContent;
        this.input.value = this.originalValue;

        this.display.style.display = 'none';
        this.input.style.display = 'block';
        this.editIcon.style.display = 'none';

        setTimeout(() => {
            this.input.focus();
            this.input.select();
        }, 0);
    }

    exitEditMode() {
        if (!this.isEditing) return;

        this.isEditing = false;
        this.input.style.display = 'none';
        this.display.style.display = 'block';
        this.editIcon.style.display = 'block';
    }

    handleKeydown(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            this.saveChanges();
        } else if (e.key === 'Escape') {
            e.preventDefault();
            this.cancelChanges();
        }
    }

    validateName(name) {
        if (!name?.trim()) {
            showToast('Folder name cannot be empty.', 'error');
            return false;
        }
        return true;
    }

    async saveChanges() {
        if (!this.isEditing) return;

        const newValue = this.input.value.trim();

        if (!this.validateName(newValue)) {
            this.input.focus();
            return;
        }

        if (newValue === this.originalValue) {
            this.exitEditMode();
            return;
        }

        try {
            await this.updateFolderName(newValue);
            this.display.textContent = newValue;
            this.updateBreadcrumb(newValue);
            this.exitEditMode();
            showToast('Folder name updated successfully.', 'success');
        } catch (error) {
            console.error('Failed to save folder name:', error);
            showToast(`Failed to save folder name: ${error.responseJSON.message}`, 'error');
            this.input.focus();
        }
    }

    async updateFolderName(newName) {
        return $.ajax({
            method: 'put',
            url: `api/dashboards/folders/${this.folderId}`,
            headers: { 'Content-Type': 'application/json' },
            data: JSON.stringify({ name: newName }),
        });
    }

    updateBreadcrumb(newName) {
        const breadcrumbName = document.querySelector('.name-dashboard');
        if (breadcrumbName) {
            breadcrumbName.textContent = newName;
        }
    }

    cancelChanges() {
        this.input.value = this.originalValue;
        this.exitEditMode();
    }

    setFolderName(name) {
        this.display.textContent = name;
    }

    getFolderName() {
        return this.display.textContent;
    }
}

function initializeFolderNameEditor(folderName, folderId) {
    const emptyDiv = document.querySelector('#folder-actions');

    if (emptyDiv) {
        emptyDiv.insertAdjacentHTML(
            'afterbegin',
            `
            <div class="folder-name-editor d-flex align-items-center" id="folderNameEditor">
                <span class="folder-name-display">${folderName}</span>
                <input type="text" class="folder-name-input" style="display: none;">
                <div class="folder-edit-icon" title="Edit folder name">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M12 20h9"></path>
                        <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"></path>
                    </svg>
                </div>
            </div>
        `
        );

        new FolderNameEditor('folderNameEditor', folderId);
    }
}
