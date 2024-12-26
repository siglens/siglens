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
        return [
            {
                headerName: 'Name',
                field: 'name',
                sortable: true,
                flex: 2,
                cellRenderer: (params) => this.nameColumnRenderer(params),
            },
            // {
            //     headerName: 'Created At',
            //     field: 'createdAt',
            //     sortable: true,
            //     flex: 1,
            //     cellStyle: { justifyContent: 'flex-end' },
            //     headerClass: 'ag-right-aligned-header',
            //     cellRenderer: (params) => this.dateCellRenderer(params)
            // },
            // {
            //     cellRenderer: 'btnRenderer',
            //     width: 150,
            // }
        ];
    }

    nameColumnRenderer(params) {
        // Calculate indentation level
        const getIndentLevel = (data) => {
            let level = 0;
            let currentData = data;
            while (currentData.parentFolderId) {
                level++;
                currentData = this.gridOptions.api.getModel().rowsToDisplay.find((row) => row.data.uniqId === currentData.parentFolderId)?.data;
                if (!currentData) break;
            }
            return level;
        };

        const indentLevel = getIndentLevel(params.data);
        const basePadding = 20; // Base padding for each level
        const indentPadding = indentLevel * basePadding;

        if (params.data.type === 'no-items') {
            const noItemsDiv = document.createElement('div');
            noItemsDiv.style.display = 'flex';
            noItemsDiv.style.alignItems = 'center';
            noItemsDiv.style.paddingLeft = `${indentPadding + 25}px`; // Indent + space for arrow
            noItemsDiv.style.color = '#666'; // Grey color
            noItemsDiv.style.fontStyle = 'italic';
            noItemsDiv.textContent = params.value;
            return noItemsDiv;
        }

        if (params.data.type === 'folder') {
            const folderDiv = document.createElement('div');
            folderDiv.className = 'folder-row';
            folderDiv.innerHTML = `
                <div style="display: flex; align-items: center; padding-left: ${indentPadding}px;">
                    <span class="folder-arrow" style="cursor: pointer">
                        ${params.data.expanded ? '<i class="fa fa-chevron-down"></i>' : '<i class="fa fa-chevron-right"></i>'}
                    </span>
                    <i class="fa fa-folder" style="color: #FFB84D; margin-right: 8px; margin-left: 8px; cursor: auto"></i>
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
            dashDiv.style.paddingLeft = `${indentPadding}px`;

            // Add empty space where arrow would be
            const spacer = document.createElement('span');
            spacer.style.width = '33px'; // Same width as folder-arrow
            spacer.style.display = 'inline-block';
            dashDiv.appendChild(spacer);

            const icon = document.createElement('i');
            icon.className = 'fa fa-columns';
            icon.style.color = '#6366f1';
            icon.style.marginRight = '8px';
            dashDiv.appendChild(icon);

            const link = document.createElement('a');
            link.href = `dashboard.html?id=${params.data.uniqId}`;
            link.innerText = params.value;
            dashDiv.appendChild(link);

            return dashDiv;
        }
    }

    dateCellRenderer(params) {
        if (!params.value || params.data.type === 'folder') return '-';
        const date = new Date(params.value);
        return date.toLocaleDateString([], {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
        });
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

    setData(items) {
        let rowData = [];
        let rowId = 0;

        items.forEach((item) => {
            rowData.push({
                rowId: rowId++,
                uniqId: item.id,
                name: item.name,
                type: item.type,
                createdAt: item.type === 'dashboard' ? item.createdAt : null,
                favorite: item.type === 'dashboard' ? item.isFavorite : null,
                isDefault: item.isDefault,
                childCount: item.type === 'folder' ? item.childCount : null,
                expanded: false,
            });
        });

        this.gridOptions.api.setColumnDefs(this.getColumnDefs());
        this.gridOptions.api.setRowData(rowData);
        this.gridOptions.api.sizeColumnsToFit();
    }

    // Additional utility methods as needed
}
