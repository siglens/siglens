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
    constructor(containerId, options = {}) {
        this.gridDiv = document.querySelector(`#${containerId}`);
        this.isSearchView = options.isSearchView || false;
        this.gridOptions = {
            columnDefs: this.getColumnDefs(),
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

    getColumnDefs() {
        if (this.isSearchView) {
            return [
                {
                    headerName: 'Name',
                    field: 'name',
                    flex: 2,
                    cellRenderer: (params) => this.nameColumnRenderer(params, false), // false for no indentation
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
                    field: 'parentPath',
                    flex: 2,
                    cellRenderer: (params) => {
                        if (!params.value) return 'Root';
                        return params.value;
                    },
                },
            ];
        } else {
            // Original tree view
            return [
                {
                    headerName: 'Name',
                    field: 'name',
                    flex: 2,
                    cellRenderer: (params) => this.nameColumnRenderer(params, true), // true for indentation
                },
            ];
        }
    }

    nameColumnRenderer(params, useIndentation) {
        if (params.data.type === 'no-items') {
            return this.renderNoItemsRow(params, useIndentation);
        }

        const baseDiv = document.createElement('div');
        baseDiv.style.display = 'flex';
        baseDiv.style.alignItems = 'center';

        if (useIndentation) {
            // Add indentation for tree view
            const indentLevel = this.getIndentLevel(params.data);
            const indentPadding = indentLevel * 20;
            baseDiv.style.paddingLeft = `${indentPadding}px`;

            if (params.data.type === 'folder') {
                return this.renderFolderRow(params, baseDiv);
            }
        }

        // Render item (both for tree view dashboards and search view items)
        const icon = document.createElement('i');
        icon.className = params.data.type === 'folder' ? 'fa fa-folder' : 'fa fa-columns';
        icon.style.color = params.data.type === 'folder' ? '#FFB84D' : '#6366f1';
        icon.style.marginRight = '8px';

        if (useIndentation) {
            const spacer = document.createElement('span');
            spacer.style.width = '33px'; // Same width as folder-arrow
            spacer.style.display = 'inline-block';
            baseDiv.appendChild(spacer);
        }

        const link = document.createElement('a');
        link.href = params.data.type === 'folder' ? `folder.html?id=${params.data.uniqId}` : `dashboard.html?id=${params.data.uniqId}`;
        link.innerText = params.value;

        baseDiv.appendChild(icon);
        baseDiv.appendChild(link);

        return baseDiv;
    }

    renderNoItemsRow(params, useIndentation) {
        const div = document.createElement('div');
        div.style.display = 'flex';
        div.style.alignItems = 'center';
        if (useIndentation) {
            const indentLevel = this.getIndentLevel(params.data);
            div.style.paddingLeft = `${indentLevel * 20 + 25}px`;
        }
        div.style.color = '#666';
        div.style.fontStyle = 'italic';
        div.textContent = params.value;
        return div;
    }

    renderFolderRow(params, baseDiv) {
        const arrowSpan = document.createElement('span');
        arrowSpan.className = 'folder-arrow';
        arrowSpan.style.cursor = 'pointer';
        arrowSpan.innerHTML = params.data.expanded ? '<i class="fa fa-chevron-down"></i>' : '<i class="fa fa-chevron-right"></i>';
        arrowSpan.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            this.toggleFolder(params);
        });

        const icon = document.createElement('i');
        icon.className = 'fa fa-folder';
        icon.style.color = '#FFB84D';
        icon.style.margin = '0 8px';

        const link = document.createElement('a');
        link.href = `folder.html?id=${params.data.uniqId}`;
        link.innerText = params.value;

        baseDiv.appendChild(arrowSpan);
        baseDiv.appendChild(icon);
        baseDiv.appendChild(link);

        return baseDiv;
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

        // Update the folder's arrow in the grid
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

        // Get folder contents from API
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

        // Process folder contents normally
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

    setData(items, isSearchResult = false) {
        console.log("isSearchResult = " + isSearchResult);
        if (isSearchResult !== this.isSearchView) {
            this.isSearchView = isSearchResult;
            this.gridOptions.api.setColumnDefs(this.getColumnDefs());
        }

        let rowData = [];
        let rowId = 0;

        items.forEach((item) => {
            rowData.push({
                rowId: rowId++,
                uniqId: item.id,
                name: item.name,
                type: item.type,
                parentPath: item.parentPath,
                parentFolderId: item.parentId,
                createdAt: item.createdAt,
                favorite: item.isFavorite,
                isDefault: item.isDefault,
                childCount: item.type === 'folder' ? item.childCount : null,
                expanded: false,
            });
        });

        this.gridOptions.api.setRowData(rowData);
        this.gridOptions.api.sizeColumnsToFit();
    }
}
