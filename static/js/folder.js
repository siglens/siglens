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

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);

    const folderId = getCurrentFolderId();
    await loadFolderContents(folderId);

    const addNew = new AddNewComponent('add-new-container');
    const grid = new DashboardGrid('dashboard-grid');

    await initializePage(grid);
    
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
        setupSearch(searchInput, grid, folderId);
    }

    $('#move-folder').click(showMoveModal);
    $('#delete-folder').click(showDeleteModal);
});

async function loadFolderContents(folderId) {
    return await getFolderContents(folderId).then((response) => {
        // Find parent from breadcrumbs (second to last item)
        const breadcrumbs = response.breadcrumbs || [];
        const parentBreadcrumb = breadcrumbs[breadcrumbs.length - 2];
        currentFolderParentId = parentBreadcrumb ? parentBreadcrumb.id : 'root-folder';

        const breadcrumb = new Breadcrumb();
        breadcrumb.render(
            response.breadcrumbs,
            response.folder.name,
            false // Don't show favorite button for folders
        );
    });
}

async function showMoveModal() {
    const response = await getFolderContents(folderId);
    const counts = countFolderContents(response);
    $('.content-count').text(`${counts.total} items: ${counts.folders} folders, ${counts.dashboards} dashboards`);
    $('.popupOverlay, #move-folder-modal').addClass('active');

    // populateTargetFolders();
}

async function showDeleteModal() {
    const response = await getFolderContents(folderId);
    const counts = countFolderContents(response);
    $('.content-count').text(`${counts.total} items: ${counts.folders} folders, ${counts.dashboards} dashboards`);

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
}

function countFolderContents(res) {
    const counts = {
        total: 0,
        folders: 0,
        dashboards: 0,
    };

    res.items.forEach((item) => {
        counts.total++;
        if (item.type === 'folder') {
            counts.folders++;
        } else {
            counts.dashboards++;
        }
    });

    return counts;
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
    });
}

function deleteFolder(folderId) {
    return $.ajax({
        method: 'delete',
        url: `api/dashboards/folders/${folderId}`,
        headers: {
            'Content-Type': 'application/json',
            Accept: '*/*',
        },
    });
}
