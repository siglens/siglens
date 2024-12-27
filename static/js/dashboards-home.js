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

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);
    const { loadData } = initializeDashboardPage();
    await loadData();
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
