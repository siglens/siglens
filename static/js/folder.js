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
}

async function showMoveModal() {
    const counts = countFolderContents(currentFolderContents);
    if (counts.total) {
        $('.content-count').text(`${counts.total} items: ${counts.folders} folders, ${counts.dashboards} dashboards`);
    } else {
        $('.content-count').text(`1 item: 1 folder`);
    }
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
    const counts = countFolderContents(currentFolderContents);
    if (counts.total) {
        $('.content-count').text(`${counts.total} items: ${counts.folders} folders, ${counts.dashboards} dashboards`);
    } else {
        $('.content-count').text(`1 item: 1 folder`);
    }

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
