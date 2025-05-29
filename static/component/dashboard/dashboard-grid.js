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
            rowHeight: 44,
            defaultColDef: {
                icons: {
                    sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
                    sortDescending: '<i class="fa fa-sort-alpha-down"/>',
                },
            },
            enableCellTextSelection: true,
            suppressScrollOnNewData: true,
            suppressAnimationFrame: true,
            rowSelection: 'multiple',
            rowMultiSelectWithClick: true,
            suppressRowDeselection: false,
            suppressRowClickSelection: true,
            getRowId: (params) => params.data.rowId,
            localeText: {
                noRowsToShow: "This folder doesn't have any dashboards/folders yet ",
            },
            onSelectionChanged: () => this.handleSelectionChange(),
        };

        this.init();
    }

    init() {
        new agGrid.Grid(this.gridDiv, this.gridOptions);
    }

    getListViewColumnDefs() {
        return [
            {
                checkboxSelection: true,
                headerCheckboxSelection: true,
                maxWidth: 50,
                pinned: 'left',
            },
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
                headerCheckboxSelection: true,
                maxWidth: 50,
                pinned: 'left',
                cellRenderer: (params) => {
                    if (params.data.type === 'no-items') {
                        return '';
                    }

                    if (params.data.type === 'folder') {
                        const div = document.createElement('div');
                        div.className = 'ag-selection-checkbox';

                        const checkbox = document.createElement('input');
                        checkbox.type = 'checkbox';
                        checkbox.className = 'ag-checkbox-input';

                        const selectionState = this.getFolderSelectionState(params.data.uniqId);

                        if (selectionState === 'all') {
                            checkbox.checked = true;
                            checkbox.indeterminate = false;
                        } else if (selectionState === 'some') {
                            checkbox.checked = false;
                            checkbox.indeterminate = true;
                        } else {
                            checkbox.checked = false;
                            checkbox.indeterminate = false;
                        }

                        checkbox.addEventListener('change', (e) => {
                            e.stopPropagation();
                            const shouldSelect = selectionState !== 'all';
                            params.node.setSelected(shouldSelect);
                        });

                        div.appendChild(checkbox);
                        return div;
                    }

                    // Default checkbox for dashboards
                    const div = document.createElement('div');
                    div.className = 'ag-selection-checkbox';

                    const checkbox = document.createElement('input');
                    checkbox.type = 'checkbox';
                    checkbox.className = 'ag-checkbox-input';
                    checkbox.checked = params.node.isSelected();

                    checkbox.addEventListener('change', (e) => {
                        e.stopPropagation();
                        params.node.setSelected(e.target.checked);
                    });

                    div.appendChild(checkbox);
                    return div;
                },
            },
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
                    div.className = 'dashboard-grid-btn';

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
        const indentPadding = indentLevel * 14;

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
            dashDiv.style.paddingLeft = `${indentPadding + 27}px`;

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
            const indentPadding = indentLevel * 20 + 17;
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
            // Collapsing
            const getAllChildIds = (parentId) => {
                const children = currentData.filter((row) => row.parentFolderId === parentId);
                let ids = children.map((child) => child.rowId);

                children.forEach((child) => {
                    if (child.type === 'folder') {
                        ids = [...ids, ...getAllChildIds(child.uniqId)];
                    }
                });

                return ids;
            };

            const idsToRemove = getAllChildIds(folderId);
            const newData = currentData.filter((row) => !idsToRemove.includes(row.rowId));
            this.gridOptions.api.setRowData(newData);
            return;
        }

        // Expanding folder
        const contents = await getFolderContents(folderId);
        if (!contents) return;

        const folderIndex = currentData.findIndex((row) => row.uniqId === folderId);

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

        const parentNode = this.gridOptions.api.getRowNode(params.data.rowId);
        if (parentNode && parentNode.isSelected()) {
            setTimeout(() => {
                this.selectDeselectChildren(folderId, true);
                setTimeout(() => {
                    this.refreshFolderCheckboxes();
                }, 50);
            }, 150);
        }
    }

    setData(items, useListView = false) {
        let rowData = [];
        let rowId = 0;

        if (!useListView) {
            items.sort((a, b) => {
                if (a.type !== b.type) {
                    return a.type === 'folder' ? -1 : 1;
                }
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
                const countData = await getFolderCount(data.uniqId);
                const message = getCountMessage(countData.total + 1, countData.folders + 1, countData.dashboards);
                $('.content-count').text(message);
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

                                    const currentData = this.gridOptions.api.getModel().rowsToDisplay.map((row) => row.data);

                                    const getAllChildIds = (parentId) => {
                                        const children = currentData.filter((row) => row.parentFolderId === parentId);
                                        let ids = children.map((child) => child.rowId);

                                        children.forEach((child) => {
                                            if (child.type === 'folder') {
                                                ids = [...ids, ...getAllChildIds(child.uniqId)];
                                            }
                                        });

                                        return ids;
                                    };

                                    const idsToRemove = [data.rowId, ...getAllChildIds(data.uniqId)];

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
    async handleBulkDelete() {
        const selectedNodes = this.gridOptions.api.getSelectedNodes();
        const selectedData = selectedNodes.map((node) => node.data);

        // Filter out indeterminate folders AND child folders of selected parents
        const actuallySelectedItems = selectedData.filter((item) => {
            if (item.type === 'folder') {
                const selectionState = this.getFolderSelectionState(item.uniqId);
                if (selectionState !== 'all') return false;

                // Check if this folder is a child of another selected folder
                const isChildOfSelectedFolder = selectedData.some((otherItem) => {
                    if (otherItem.type === 'folder' && otherItem.uniqId !== item.uniqId) {
                        const otherSelectionState = this.getFolderSelectionState(otherItem.uniqId);
                        if (otherSelectionState === 'all') {
                            return this.isChildOf(item.uniqId, otherItem.uniqId);
                        }
                    }
                    return false;
                });

                return !isChildOfSelectedFolder;
            }
            return true; // Always include dashboards for now, filter later
        });

        // Get list of fully selected folder IDs to avoid double counting their children
        const fullySelectedFolderIds = actuallySelectedItems.filter((item) => item.type === 'folder').map((item) => item.uniqId);

        let folderCount = 0;
        let dashboardCount = 0;

        for (const item of actuallySelectedItems) {
            if (item.type === 'folder') {
                const countData = await getFolderCount(item.uniqId);
                if (countData && countData.total > 0) {
                    folderCount += countData.folders + 1;
                    dashboardCount += countData.dashboards;
                } else {
                    folderCount += 1;
                }
            } else {
                // Only count dashboards that are NOT children of fully selected folders
                const isChildOfSelectedFolder = fullySelectedFolderIds.some((folderId) => {
                    return this.isChildOf(item.uniqId, folderId);
                });

                if (!isChildOfSelectedFolder) {
                    dashboardCount += 1;
                }
            }
        }

        const totalCount = folderCount + dashboardCount;

        const message = getCountMessage(totalCount, folderCount, dashboardCount);
        $('.content-count').text(message);

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
            .on('click', async () => {
                if ($('.confirm-input').val() === 'Delete') {
                    try {
                        const currentData = this.gridOptions.api.getModel().rowsToDisplay.map((row) => row.data);

                        const getAllChildIds = (parentId) => {
                            const children = currentData.filter((row) => row.parentFolderId === parentId);
                            let ids = children.map((child) => child.rowId);

                            children.forEach((child) => {
                                if (child.type === 'folder') {
                                    ids = [...ids, ...getAllChildIds(child.uniqId)];
                                }
                            });

                            return ids;
                        };

                        // Collect all IDs to remove from UI (selected items + their nested children)
                        let allIdsToRemove = [];

                        for (const item of selectedData) {
                            allIdsToRemove.push(item.rowId);

                            if (item.type === 'folder') {
                                const childIds = getAllChildIds(item.uniqId);
                                allIdsToRemove = [...allIdsToRemove, ...childIds];
                            }
                        }

                        // Only delete items that aren't children of other selected folders
                        for (const item of selectedData) {
                            const isChildOfSelectedFolder = selectedData.some((otherItem) => {
                                if (otherItem.type === 'folder' && otherItem.uniqId !== item.uniqId) {
                                    return this.isChildOf(item.uniqId, otherItem.uniqId);
                                }
                                return false;
                            });

                            if (!isChildOfSelectedFolder) {
                                if (item.type === 'folder') {
                                    await deleteFolder(item.uniqId);
                                } else {
                                    await deleteDashboard(item.uniqId);
                                }
                            }
                        }

                        allIdsToRemove = [...new Set(allIdsToRemove)];

                        this.gridOptions.api.applyTransaction({
                            remove: allIdsToRemove.map((id) => ({ rowId: id })),
                        });

                        $('.popupOverlay, #delete-folder-modal').removeClass('active');
                        this.clearSelection();
                    } catch (error) {
                        console.error('Error deleting items:', error);
                        alert('Failed to delete some items. Please try again.');
                    }
                }
            });

        $('.cancel-btn').click(function () {
            $('.popupOverlay, .popupContent').removeClass('active');
            $('.confirm-input').val('');
        });
    }

    getFolderSelectionState(folderId) {
        const allNodes = [];
        this.gridOptions.api.forEachNode((node) => allNodes.push(node));

        const getChildNodes = (parentId) => {
            const visibleChildren = allNodes.filter((node) => node.data.parentFolderId === parentId && node.data.type !== 'no-items');
            let childNodes = [...visibleChildren];

            visibleChildren.forEach((child) => {
                if (child.data.type === 'folder') {
                    childNodes = [...childNodes, ...getChildNodes(child.data.uniqId)];
                }
            });

            return childNodes;
        };

        const childNodes = getChildNodes(folderId);
        const selectedChildren = childNodes.filter((node) => node.isSelected());

        if (childNodes.length === 0) {
            const folderNode = allNodes.find((node) => node.data.uniqId === folderId);
            return folderNode && folderNode.isSelected() ? 'all' : 'none';
        }

        if (selectedChildren.length === 0) return 'none';
        if (selectedChildren.length === childNodes.length) return 'all';
        return 'some';
    }

    handleSelectionChange() {
        const selectedNodes = this.gridOptions.api.getSelectedNodes();
        const selectedCount = selectedNodes.length;

        this.handleCascadingSelection();

        setTimeout(() => {
            this.refreshFolderCheckboxes();
        }, 50);

        if (selectedCount > 0) {
            $('#bulk-delete-btn').show().text(`Delete`);
            $('.filter-controls, #sort-container').hide();

            $('#bulk-delete-btn')
                .off('click')
                .on('click', () => this.handleBulkDelete());
        } else {
            $('#bulk-delete-btn').hide();
            $('.filter-controls, #sort-container').show();
        }
    }

    refreshFolderCheckboxes() {
        this.gridOptions.api.refreshCells({
            force: true,
        });
    }

    handleCascadingSelection() {
        const allNodes = [];
        this.gridOptions.api.forEachNode((node) => allNodes.push(node));

        const currentSelection = new Set();
        const previousSelection = this.previousSelection || new Set();

        allNodes.forEach((node) => {
            if (node.isSelected()) {
                currentSelection.add(node.data.rowId);
            }
        });

        const newlySelected = [...currentSelection].filter((id) => !previousSelection.has(id));
        const newlyDeselected = [...previousSelection].filter((id) => !currentSelection.has(id));

        newlySelected.forEach((rowId) => {
            const node = this.gridOptions.api.getRowNode(rowId);
            if (node && node.data.type === 'folder') {
                this.selectDeselectChildren(node.data.uniqId, true);
            }
        });

        newlyDeselected.forEach((rowId) => {
            const nodeData = allNodes.find((n) => n.data.rowId === rowId)?.data;
            if (nodeData && nodeData.type === 'folder') {
                this.selectDeselectChildren(nodeData.uniqId, false);
            }
        });

        this.updateParentFolderStates(allNodes);

        this.previousSelection = new Set(currentSelection);
    }

    updateParentFolderStates(allNodes) {
        const folders = allNodes.filter((node) => node.data.type === 'folder');

        folders.forEach((folderNode) => {
            const folderId = folderNode.data.uniqId;
            const selectionState = this.getFolderSelectionState(folderId);

            if (selectionState === 'all') {
                if (!folderNode.isSelected()) {
                    folderNode.setSelected(true, false);
                }
            } else if (selectionState === 'none') {
                if (folderNode.isSelected()) {
                    folderNode.setSelected(false, false);
                }
            }
        });
    }

    selectDeselectChildren(folderId, select) {
        const allNodes = [];
        this.gridOptions.api.forEachNode((node) => allNodes.push(node));

        const getChildNodes = (parentId) => {
            const children = allNodes.filter((node) => node.data.parentFolderId === parentId);
            let childNodes = [...children];

            children.forEach((child) => {
                if (child.data.type === 'folder') {
                    childNodes = [...childNodes, ...getChildNodes(child.data.uniqId)];
                }
            });

            return childNodes;
        };

        const childNodes = getChildNodes(folderId);

        childNodes.forEach((childNode) => {
            if (childNode.data.type !== 'no-items') {
                childNode.setSelected(select, false);
            }
        });

        setTimeout(() => {
            this.gridOptions.api.refreshCells({
                force: true,
            });
        }, 10);
    }

    clearSelection() {
        this.gridOptions.api.deselectAll();
    }

    isChildOf(childId, parentFolderId) {
        const allNodes = [];
        this.gridOptions.api.forEachNode((node) => allNodes.push(node));

        const childNode = allNodes.find((node) => node.data.uniqId === childId);
        if (!childNode) return false;

        let currentParentId = childNode.data.parentFolderId;
        while (currentParentId) {
            if (currentParentId === parentFolderId) return true;

            const parentNode = allNodes.find((node) => node.data.uniqId === currentParentId);
            currentParentId = parentNode?.data.parentFolderId;
        }

        return false;
    }
}
