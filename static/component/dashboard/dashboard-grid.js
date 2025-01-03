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

class DashboardGrid {
    constructor(containerId) {
        this.gridDiv = document.querySelector(`#${containerId}`);
        this.gridOptions = {
            columnDefs: this.getTreeViewColumnDefs(), // tree view by default
            rowData: [],
            animateRows: true,
            rowHeight: 54,
            defaultColDef: {
                icons: {
                    sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
                    sortDescending: '<i class="fa fa-sort-alpha-down"/>',
                },
            },
            enableCellTextSelection: true,
            suppressScrollOnNewData: true,
            suppressAnimationFrame: true,
            getRowId: (params) => params.data.rowId,
            localeText: {
                noRowsToShow: "This folder doesn't have any dashboards/folders yet ",
            },
        };

        this.init();
    }

    init() {
        new agGrid.Grid(this.gridDiv, this.gridOptions);
    }

    getListViewColumnDefs() {
        return [
            {
                headerName: 'Name',
                field: 'name',
                flex: 2,
                cellRenderer: (params) => this.nameColumnRenderer(params, false),
            },
            {
                headerName: 'Type',
                field: 'type',
                flex: 1,
                cellRenderer: (params) => {
                    const icon = params.value === 'folder' ? '<i class="fa fa-folder" style="color: #FFB84D"></i>' : '<i class="fa fa-columns" style="color: #6366f1"></i>';
                    return `<div style="display: flex; align-items: center; gap: 8px;">
                        ${icon} 
                        <span>${params.value.charAt(0).toUpperCase() + params.value.slice(1)}</span>
                    </div>`;
                },
            },
            {
                headerName: 'Location',
                field: 'parentName',
                flex: 2,
                cellRenderer: (params) => {
                    return params.value;
                },
            },
        ];
    }

    getTreeViewColumnDefs() {
        return [
            {
                headerName: 'Name',
                field: 'name',
                cellRenderer: (params) => this.nameColumnRenderer(params, true),
            },
            {
                headerName: 'Actions',
                field: 'actions',
                width: 100,
                cellRenderer: (params) => {
                    if (params.data.type === 'no-items') {
                        return '';
                    }

                    const div = document.createElement('div');
                    div.id = 'dashboard-grid-btn';

                    if (params.data.isDefault) {
                        div.innerHTML = `
                            <span class="default-label" style="text-decoration: none;">
                                Default
                            </span>
                        `;
                    } else {
                        div.innerHTML = `
                            <button class="btn-simple" id="delbutton" title="Delete dashboard"></button>
                        `;

                        const deleteButton = div.querySelector('.btn-simple');
                        deleteButton.onclick = (e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            this.handleDelete(params.data);
                        };
                    }

                    return div;
                },
            },
        ];
    }

    nameColumnRenderer(params, useIndentation) {
        // For list view
        if (!useIndentation) {
            const baseDiv = document.createElement('div');
            baseDiv.style.display = 'flex';
            baseDiv.style.alignItems = 'center';

            const icon = document.createElement('i');
            icon.className = params.data.type === 'folder' ? 'fa fa-folder' : 'fa fa-columns';
            icon.style.color = params.data.type === 'folder' ? '#FFB84D' : '#6366f1';
            icon.style.marginRight = '8px';

            const link = document.createElement('a');
            link.href = params.data.type === 'folder' ? `folder.html?id=${params.data.uniqId}` : `dashboard.html?id=${params.data.uniqId}`;
            link.innerText = params.value;

            baseDiv.appendChild(icon);
            baseDiv.appendChild(link);
            return baseDiv;
        }

        // For tree view
        if (params.data.type === 'no-items') {
            return this.renderNoItemsRow(params, useIndentation);
        }

        const indentLevel = this.getIndentLevel(params.data);
        const indentPadding = indentLevel * 20;

        if (params.data.type === 'folder') {
            const folderDiv = document.createElement('div');
            folderDiv.className = 'folder-row';
            folderDiv.innerHTML = `
                <div style="display: flex; align-items: center; padding-left: ${indentPadding}px;">
                    <span class="folder-arrow" style="cursor: pointer">
                        ${params.data.expanded ? '<i class="fa fa-chevron-down"></i>' : '<i class="fa fa-chevron-right"></i>'}
                    </span>
                    <i class="fa fa-folder" style="color: #FFB84D; margin-right: 8px; margin-left: 8px;"></i>
                    <a href="folder.html?id=${params.data.uniqId}">${params.value}</a>
                </div>`;

            const arrowElement = folderDiv.querySelector('.folder-arrow');
            arrowElement.addEventListener('click', async (event) => {
                event.preventDefault();
                event.stopPropagation();
                await this.toggleFolder(params);
            });

            return folderDiv;
        } else {
            const dashDiv = document.createElement('div');
            dashDiv.style.display = 'flex';
            dashDiv.style.alignItems = 'center';
            dashDiv.style.paddingLeft = `${indentPadding + 33}px`;

            const icon = document.createElement('i');
            icon.className = 'fa fa-columns';
            icon.style.color = '#6366f1';
            icon.style.marginRight = '8px';

            const link = document.createElement('a');
            link.href = `dashboard.html?id=${params.data.uniqId}`;
            link.innerText = params.value;

            dashDiv.appendChild(icon);
            dashDiv.appendChild(link);

            return dashDiv;
        }
    }

    renderNoItemsRow(params, useIndentation) {
        const div = document.createElement('div');
        div.style.display = 'flex';
        div.style.alignItems = 'center';
        if (useIndentation) {
            const indentLevel = this.getIndentLevel(params.data);
            const indentPadding = indentLevel * 20 + 33;
            div.style.paddingLeft = `${indentPadding}px`;
        }
        div.style.color = '#666';
        div.style.fontStyle = 'italic';
        div.textContent = params.value;
        return div;
    }

    getIndentLevel(data) {
        let level = 0;
        let currentData = data;
        while (currentData.parentFolderId) {
            level++;
            currentData = this.gridOptions.api.getModel().rowsToDisplay.find((row) => row.data.uniqId === currentData.parentFolderId)?.data;
            if (!currentData) break;
        }
        return level;
    }

    async toggleFolder(params) {
        const folderId = params.data.uniqId;
        const currentData = this.gridOptions.api.getModel().rowsToDisplay.map((row) => row.data);

        // Toggle expanded state
        params.data.expanded = !params.data.expanded;

        const node = this.gridOptions.api.getRowNode(params.data.rowId);
        if (node) {
            node.setData({ ...params.data });
        }

        if (!params.data.expanded) {
            // Recursively get all child IDs to remove
            const getAllChildIds = (parentId) => {
                const children = currentData.filter((row) => row.parentFolderId === parentId);
                let ids = children.map((child) => child.rowId);

                // Recursively get children of folders
                children.forEach((child) => {
                    if (child.type === 'folder') {
                        ids = [...ids, ...getAllChildIds(child.uniqId)];
                    }
                });

                return ids;
            };

            // Get all nested items to remove
            const idsToRemove = getAllChildIds(folderId);
            const newData = currentData.filter((row) => !idsToRemove.includes(row.rowId));
            this.gridOptions.api.setRowData(newData);
            return;
        }

        const contents = await getFolderContents(folderId);
        if (!contents) return;

        // Find the index of the folder
        const folderIndex = currentData.findIndex((row) => row.uniqId === folderId);

        // If folder is empty, add "No items" row
        if (!contents.items || contents.items.length === 0) {
            const noItemsRow = {
                rowId: `${folderId}-no-items`,
                uniqId: `${folderId}-no-items`,
                name: 'No items',
                type: 'no-items',
                inFolder: true,
                parentFolderId: folderId,
                isNoItems: true,
            };

            const newData = [...currentData.slice(0, folderIndex + 1), noItemsRow, ...currentData.slice(folderIndex + 1)];
            this.gridOptions.api.setRowData(newData);
            return;
        }

        const folderContents = contents.items.map((item, index) => ({
            rowId: `${folderId}-${index}`,
            uniqId: item.id,
            name: item.name,
            type: item.type,
            inFolder: true,
            parentFolderId: folderId,
            createdAt: item.type === 'dashboard' ? item.createdAt : null,
            favorite: item.type === 'dashboard' ? item.isFavorite : null,
            isDefault: item.isDefault,
            childCount: item.type === 'folder' ? item.childCount : null,
            expanded: false,
        }));

        const newData = [...currentData.slice(0, folderIndex + 1), ...folderContents, ...currentData.slice(folderIndex + 1)];

        this.gridOptions.api.setRowData(newData);
    }

    setData(items, useListView = false) {
        let rowData = [];
        let rowId = 0;

        // If tree view, sort items by type first (folders before dashboards)
        if (!useListView) {
            items.sort((a, b) => {
                if (a.type !== b.type) {
                    return a.type === 'folder' ? -1 : 1;
                }
                // If same type, maintain original order
                return 0;
            });
        }

        items.forEach((item) => {
            rowData.push({
                rowId: rowId++,
                uniqId: item.id,
                name: item.name,
                type: item.type,
                parentName: item.parentName,
                parentFolderId: item.parentId,
                createdAt: item.createdAt,
                favorite: item.isFavorite,
                isDefault: item.isDefault,
                childCount: item.type === 'folder' ? item.childCount : null,
                expanded: false,
            });
        });

        this.gridOptions.api.setRowData([]);

        const columnDefs = useListView ? this.getListViewColumnDefs() : this.getTreeViewColumnDefs();
        this.gridOptions.api.setColumnDefs(columnDefs);
        this.gridOptions.api.setRowData(rowData);
        this.gridOptions.api.sizeColumnsToFit();
    }

    async handleDelete(data) {
        try {
            if (data.type === 'folder') {
                const contents = await getFolderContents(data.uniqId);
                const counts = countFolderContents(contents);
                if (counts.total) {
                    $('.content-count').text(`${counts.total} items: ${counts.folders} folders, ${counts.dashboards} dashboards`);
                } else {
                    $('.content-count').text(`1 item: 1 folder`);
                }
            } else {
                $('.content-count').text('1 item: 1 dashboard');
            }

            $('.popupOverlay, #delete-folder-modal').addClass('active');

            $('.confirm-input')
                .val('')
                .off('input')
                .on('input', function () {
                    $('.delete-btn').prop('disabled', $(this).val() !== 'Delete');
                });

            $('.delete-btn')
                .prop('disabled', true)
                .off('click')
                .on(
                    'click',
                    async function () {
                        if ($('.confirm-input').val() === 'Delete') {
                            try {
                                if (data.type === 'folder') {
                                    await deleteFolder(data.uniqId);

                                    // Get all visible rows
                                    const currentData = this.gridOptions.api.getModel().rowsToDisplay.map((row) => row.data);

                                    // Recursively get all child IDs to remove
                                    const getAllChildIds = (parentId) => {
                                        const children = currentData.filter((row) => row.parentFolderId === parentId);
                                        let ids = children.map((child) => child.rowId);

                                        // Recursively get children of folders
                                        children.forEach((child) => {
                                            if (child.type === 'folder') {
                                                ids = [...ids, ...getAllChildIds(child.uniqId)];
                                            }
                                        });

                                        return ids;
                                    };

                                    // Get all items to remove
                                    const idsToRemove = [data.rowId, ...getAllChildIds(data.uniqId)];

                                    // Remove parent and all children
                                    this.gridOptions.api.applyTransaction({
                                        remove: idsToRemove.map((id) => ({ rowId: id })),
                                    });
                                } else {
                                    await deleteDashboard(data.uniqId);
                                    this.gridOptions.api.applyTransaction({
                                        remove: [{ rowId: data.rowId }],
                                    });
                                }

                                $('.popupOverlay, #delete-folder-modal').removeClass('active');
                            } catch (error) {
                                console.error('Error deleting:', error);
                                alert('Failed to delete. Please try again.');
                            }
                        }
                    }.bind(this)
                );

            $('.cancel-btn').click(function () {
                $('.popupOverlay, .popupContent').removeClass('active');
                $('.confirm-input').val('');
            });
        } catch (error) {
            showToast('Failed to get item contents. Please try again.', 'error');
        }
    }
}
