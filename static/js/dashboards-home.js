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

let dbgridDiv = null;
let dbRowData = [];

let initialDashboards = null;

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);
    const addNew = new AddNewComponent('add-new-container');

    const grid = new DashboardGrid('dashboard-grid');
    const folderContents = await getFolderContents("root-folder");
    grid.setData(folderContents.items);

});

// class btnRenderer {
//     init(params) {
//         const starOutlineURL = 'url("../assets/star-outline.svg")';
//         const starFilledURL = 'url("../assets/star-filled.svg")';

//         this.eGui = document.createElement('span');
//         this.eGui.innerHTML = `<div id="dashboard-grid-btn" style="margin-left: 20px;">
//                 <button class="btn-simple" id="delbutton" title="Delete dashboard"></button>
//                 <button class="btn-duplicate" id="duplicateButton" title="Duplicate dashboard"></button>
//                 <button class="star-icon" id="favbutton" title="Mark as favorite"></button>
//             </div>`;

//         this.dButton = this.eGui.querySelector('.btn-simple');
//         this.dButton.style.marginRight = '5px';
//         this.duplicateButton = this.eGui.querySelector('.btn-duplicate');
//         this.starIcon = this.eGui.querySelector('.star-icon');
//         this.starIcon.style.backgroundImage = params.data.favorite ? starFilledURL : starOutlineURL;

//         //Disable delete for default dashboards and show "Default" label
//         if (params.data.isDefault) {
//             const defaultLabel = document.createElement('span');
//             defaultLabel.className = 'default-label';
//             defaultLabel.innerText = 'Default';
//             defaultLabel.style.textDecoration = 'none';
//             this.dButton.style.display = 'none';
//             this.duplicateButton.parentNode.insertBefore(defaultLabel, this.duplicateButton);
//         }

//         function deletedb() {
//             $.ajax({
//                 method: 'get',
//                 url: 'api/dashboards/delete/' + params.data.uniqId,
//                 headers: {
//                     'Content-Type': 'application/json; charset=utf-8',
//                     Accept: '*/*',
//                 },
//                 crossDomain: true,
//             }).then(function () {
//                 let deletedRowID = params.data.rowId;
//                 dbgridOptions.api.applyTransaction({
//                     remove: [{ rowId: deletedRowID }],
//                 });
//             });
//         }

//         function duplicatedb() {
//             $.ajax({
//                 method: 'get',
//                 url: 'api/dashboards/' + params.data.uniqId,
//                 headers: {
//                     'Content-Type': 'application/json; charset=utf-8',
//                     Accept: '*/*',
//                 },
//                 crossDomain: true,
//                 dataType: 'json',
//             }).then(function (res) {
//                 let duplicatedDBName = res.name + '-Copy';
//                 let duplicatedDescription = res.description;
//                 let duplicatedPanels = res.panels;
//                 let duplicateTimeRange = res.timeRange;
//                 let duplicateRefresh = res.refresh;
//                 let uniqIDdb;
//                 $.ajax({
//                     method: 'post',
//                     url: 'api/dashboards/create',
//                     headers: {
//                         'Content-Type': 'application/json; charset=utf-8',
//                         Accept: '*/*',
//                     },
//                     data: JSON.stringify(duplicatedDBName),
//                     dataType: 'json',
//                     crossDomain: true,
//                 })
//                     .then((res) => {
//                         uniqIDdb = Object.keys(res)[0];
//                         $.ajax({
//                             method: 'POST',
//                             url: '/api/dashboards/update',
//                             data: JSON.stringify({
//                                 id: uniqIDdb,
//                                 name: duplicatedDBName,
//                                 details: {
//                                     name: duplicatedDBName,
//                                     description: duplicatedDescription,
//                                     panels: duplicatedPanels.map((panel) => ({
//                                         ...panel,
//                                         style: {
//                                             display: panel.style?.display || 'Line chart',
//                                             color: panel.style?.color || 'Classic',
//                                             lineStyle: panel.style?.lineStyle || 'Solid',
//                                             lineStroke: panel.style?.lineStroke || 'Normal',
//                                         },
//                                     })),
//                                     timeRange: duplicateTimeRange,
//                                     refresh: duplicateRefresh,
//                                 },
//                             }),
//                         });
//                     })
//                     .then(function () {
//                         dbgridOptions.api.applyTransaction({
//                             add: [
//                                 {
//                                     dbname: duplicatedDBName,
//                                     uniqId: uniqIDdb,
//                                     createdAt: Date.now(),
//                                     favorite: false,
//                                     isDefault: false,
//                                 },
//                             ],
//                         });
//                     });
//             });
//         }

//         function toggleFavorite() {
//             $.ajax({
//                 method: 'put',
//                 url: 'api/dashboards/favorite/' + params.data.uniqId,
//                 headers: {
//                     'Content-Type': 'application/json; charset=utf-8',
//                     Accept: '*/*',
//                 },
//                 crossDomain: true,
//             }).then((response) => {
//                 params.data.favorite = response.isFavorite;
//                 this.starIcon.style.backgroundImage = params.data.favorite ? starFilledURL : starOutlineURL;
//             });
//         }

//         function showPrompt() {
//             // $('#delete-db-prompt').css('display', 'flex');
//             $('.popupOverlay, #delete-db-prompt').addClass('active');
//             // $('#new-dashboard-modal').hide();

//             $('#cancel-db-prompt, .popupOverlay').off('click');
//             $('#delete-dbbtn').off('click');

//             $('#cancel-db-prompt, .popupOverlay').click(function () {
//                 $('.popupOverlay, #delete-db-prompt').removeClass('active');
//                 // $('#delete-db-prompt').hide();
//             });

//             $('#delete-dbbtn').click(function () {
//                 deletedb();
//                 $('.popupOverlay, #delete-db-prompt').removeClass('active');
//                 // $('#delete-db-prompt').hide();
//             });
//         }

//         this.dButton.addEventListener('click', showPrompt);
//         this.duplicateButton.addEventListener('click', duplicatedb);
//         this.starIcon.addEventListener('click', toggleFavorite.bind(this));
//     }

//     getGui() {
//         return this.eGui;
//     }

//     refresh(params) {
//         const starOutlineURL = 'url("../assets/star-outline.svg")';
//         const starFilledURL = 'url("../assets/star-filled.svg")';
//         this.starIcon.style.backgroundImage = params.data.favorite ? starFilledURL : starOutlineURL;
//         return false;
//     }
// }

// let dashboardColumnDefs = [
//     {
//         field: 'rowId',
//         hide: true,
//     },
//     {
//         headerName: 'Name',
//         field: 'name',
//         sortable: true,
//         flex: 2,
//         cellRenderer: (params) => {
//             // Calculate indentation level
//             const getIndentLevel = (data) => {
//                 let level = 0;
//                 let currentData = data;
//                 while (currentData.parentFolderId) {
//                     level++;
//                     currentData = this.gridOptions.api.getModel().rowsToDisplay.find((row) => row.data.uniqId === currentData.parentFolderId)?.data;
//                     if (!currentData) break;
//                 }
//                 return level;
//             };

//             const indentLevel = getIndentLevel(params.data);
//             const basePadding = 20; // Base padding for each level
//             const indentPadding = indentLevel * basePadding;

//             if (params.data.type === 'folder') {
//                 const folderDiv = document.createElement('div');
//                 folderDiv.className = 'folder-row';
//                 folderDiv.innerHTML = `
//                     <div style="display: flex; align-items: center; padding-left: ${indentPadding}px;">
//                         <span class="folder-arrow" style="cursor: pointer">
//                             ${params.data.expanded ? '<i class="fa fa-chevron-down"></i>' : '<i class="fa fa-chevron-right"></i>'}
//                         </span>
//                         <i class="fa fa-folder" style="color: #FFB84D; margin-right: 5px;"></i>
//                         <a href="folder.html?id=${params.data.uniqId}" class="folder-name">${params.value}</a>
//                     </div>`;

//                 const arrowElement = folderDiv.querySelector('.folder-arrow');
//                 arrowElement.addEventListener('click', async (event) => {
//                     event.preventDefault();
//                     event.stopPropagation();
//                     await this.toggleFolder(params);
//                 });

//                 return folderDiv;
//             } else {
//                 const dashDiv = document.createElement('div');
//                 dashDiv.style.display = 'flex';
//                 dashDiv.style.alignItems = 'center';
//                 dashDiv.style.paddingLeft = `${indentPadding}px`; // Same padding as folders at same level

//                 const icon = document.createElement('i');
//                 icon.className = 'fa fa-columns';
//                 icon.style.color = '#6366f1';
//                 icon.style.marginRight = '5px';
//                 dashDiv.appendChild(icon);

//                 const link = document.createElement('a');
//                 link.href = `dashboard.html?id=${params.data.uniqId}`;
//                 link.innerText = params.value;

//                 dashDiv.appendChild(link);
//                 return dashDiv;
//             }
//         },
//     },
//     {
//         headerName: 'Created At',
//         field: 'createdAt',
//         sortable: true,
//         flex: 1,
//         cellStyle: { justifyContent: 'flex-end' },
//         headerClass: 'ag-right-aligned-header',
//         cellRenderer: (params) => {
//             if (!params.value || params.data.type === 'folder') return '-';
//             const date = new Date(params.value);
//             return date.toLocaleDateString([], {
//                 year: 'numeric',
//                 month: 'short',
//                 day: 'numeric',
//                 hour: '2-digit',
//                 minute: '2-digit',
//             });
//         },
//     },
//     {
//         cellRenderer: btnRenderer,
//         width: 150,
//     },
// ];

// function view(dashboardId) {
//     $.ajax({
//         method: 'get',
//         url: 'api/dashboards/' + dashboardId,
//         headers: {
//             'Content-Type': 'application/json; charset=utf-8',
//             Accept: '*/*',
//         },
//         crossDomain: true,
//         dataType: 'json',
//     }).then(function (_res) {
//         var queryString = '?id=' + dashboardId;
//         window.location.href = '../dashboard.html' + queryString;
//     });
// }

// // let the grid know which columns and what data to use
// const dbgridOptions = {
//     columnDefs: dashboardColumnDefs,
//     rowData: dbRowData,
//     animateRows: true,
//     rowHeight: 54,
//     defaultColDef: {
//         icons: {
//             sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
//             sortDescending: '<i class="fa fa-sort-alpha-down"/>',
//         },
//     },
//     enableCellTextSelection: true,
//     suppressScrollOnNewData: true,
//     suppressAnimationFrame: true,
//     getRowId: (params) => params.data.rowId,
//     onGridReady(params) {
//         this.gridApi = params.api; // To access the grids API
//     },
// };
// async function toggleFolder(params) {
//     const folderId = params.data.uniqId;
//     const currentData = dbgridOptions.api.getModel().rowsToDisplay.map((row) => row.data);

//     // Toggle expanded state
//     params.data.expanded = !params.data.expanded;

//     // Update the folder's arrow in the grid
//     const node = dbgridOptions.api.getRowNode(params.data.rowId);
//     if (node) {
//         node.setData({ ...params.data });
//     }

//     if (!params.data.expanded) {
//         // Recursively get all child IDs to remove
//         const getAllChildIds = (parentId) => {
//             const children = currentData.filter((row) => row.parentFolderId === parentId);
//             let ids = children.map((child) => child.rowId);

//             // Recursively get children of folders
//             children.forEach((child) => {
//                 if (child.type === 'folder') {
//                     ids = [...ids, ...getAllChildIds(child.uniqId)];
//                 }
//             });

//             return ids;
//         };

//         // Get all nested items to remove
//         const idsToRemove = getAllChildIds(folderId);
//         const newData = currentData.filter((row) => !idsToRemove.includes(row.rowId));
//         dbgridOptions.api.setRowData(newData);
//         return;
//     }

//     // Get folder contents from API
//     const contents = await getFolderContents(folderId);
//     if (!contents) return;

//     // Find the index of the folder
//     const folderIndex = currentData.findIndex((row) => row.uniqId === folderId);

//     // If folder is empty, add "No items" row
//     if (!contents.items || contents.items.length === 0) {
//         const noItemsRow = {
//             rowId: `${folderId}-no-items`,
//             uniqId: `${folderId}-no-items`,
//             name: 'No items',
//             type: 'no-items',
//             inFolder: true,
//             parentFolderId: folderId,
//             isNoItems: true,
//         };

//         const newData = [...currentData.slice(0, folderIndex + 1), noItemsRow, ...currentData.slice(folderIndex + 1)];
//         dbgridOptions.api.setRowData(newData);
//         return;
//     }

//     // Process folder contents normally
//     const folderContents = contents.items.map((item, index) => ({
//         rowId: `${folderId}-${index}`,
//         uniqId: item.id,
//         name: item.name,
//         type: item.type,
//         inFolder: true,
//         parentFolderId: folderId,
//         createdAt: item.type === 'dashboard' ? item.createdAt : null,
//         favorite: item.type === 'dashboard' ? item.isFavorite : null,
//         isDefault: item.isDefault,
//         childCount: item.type === 'folder' ? item.childCount : null,
//         expanded: false,
//     }));

//     const newData = [...currentData.slice(0, folderIndex + 1), ...folderContents, ...currentData.slice(folderIndex + 1)];

//     dbgridOptions.api.setRowData(newData);
// }
// async function displayDashboards() {
//     // Get root folder contents
//     const response = await getFolderContents('root-folder');
//     if (!response) return;

//     let rowData = [];
//     let rowId = 0;

//     // Process items from response
//     response.items.forEach((item) => {
//         if (item.type === 'folder') {
//             rowData.push({
//                 rowId: rowId++,
//                 uniqId: item.id,
//                 name: item.name,
//                 type: 'folder',
//                 expanded: false,
//                 isDefault: item.isDefault,
//                 childCount: item.childCount,
//             });
//         } else {
//             rowData.push({
//                 rowId: rowId++,
//                 uniqId: item.id,
//                 name: item.name,
//                 type: 'dashboard',
//                 // createdAt: item.createdAt,
//                 favorite: item.isFavorite,
//                 isDefault: item.isDefault,
//             });
//         }
//     });

//     // Initialize or update grid
//     if (dbgridDiv === null) {
//         dbgridDiv = document.querySelector('#dashboard-grid');
//         new agGrid.Grid(dbgridDiv, dbgridOptions);
//     }

//     dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
//     dbgridOptions.api.setRowData(rowData);
//     dbgridOptions.api.sizeColumnsToFit();
// }
