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

async function getAllDashboards() {
    let serverResponse = [];
    await $.ajax({
        method: 'get',
        url: 'api/dashboards/listall',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    }).then(function (res) {
        serverResponse = res;
    });
    return serverResponse;
}

async function getAllDefaultDashboards() {
    let serverResponse = [];
    await $.ajax({
        method: 'get',
        url: 'api/dashboards/defaultlistall',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    }).then(function (res) {
        serverResponse = res;
    });
    return serverResponse;
}
async function getAllFavoriteDashboards() {
    let serverResponse = [];
    await $.ajax({
        method: 'get',
        url: 'api/dashboards/listfavorites',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    }).then(function (res) {
        serverResponse = res;
    });
    return serverResponse;
}

function createDashboard() {
    $('.popupOverlay, .popupContent').addClass('active');
    $('#new-dashboard-modal').show();
    $('#delete-db-prompt').hide();

    function createDashboardWithInput() {
        var inputdbname = $('#db-name').val();
        var inputdbdescription = $('#db-description').val();
        var timeRange = 'Last 1 Hour';
        var refresh = '';

        if (!inputdbname) {
            $('.error-tip').addClass('active');
            $('.popupOverlay, .popupContent').addClass('active');
            $('#new-dashboard-modal').show();
        } else {
            $('#save-dbbtn').off('click');
            $(document).off('keypress');

            $.ajax({
                method: 'post',
                url: 'api/dashboards/create',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                data: JSON.stringify(inputdbname),
                dataType: 'json',
                crossDomain: true,
            })
                .then(function (res) {
                    $('#db-name').val('');
                    $('#db-description').val('');
                    $('.error-tip').removeClass('active');
                    $('.popupOverlay, .popupContent').removeClass('active');

                    var updateDashboard = {
                        id: Object.keys(res)[0],
                        name: Object.values(res)[0],
                        details: {
                            name: Object.values(res)[0],
                            description: inputdbdescription,
                            timeRange: timeRange,
                            refresh: refresh,
                        },
                    };

                    $.ajax({
                        method: 'post',
                        url: 'api/dashboards/update',
                        headers: {
                            'Content-Type': 'application/json; charset=utf-8',
                            Accept: '*/*',
                        },
                        data: JSON.stringify(updateDashboard),
                        dataType: 'json',
                        crossDomain: true,
                    }).then(function (msg) {
                        console.log('done:', msg);
                    });

                    var queryString = '?id=' + Object.keys(res)[0];
                    window.location.href = '../dashboard.html' + queryString;
                })
                .catch(function (updateError) {
                    if (updateError.status === 409) {
                        $('.error-tip').text('Dashboard name already exists!');
                        $('.error-tip').addClass('active');
                        $('.popupOverlay, .popupContent').addClass('active');
                        attachEventHandlers();
                    }
                });
        }
    }
    // method to attach event handlers to avoid redundant event handlers
    function attachEventHandlers() {
        $('#save-dbbtn').on('click', function () {
            createDashboardWithInput();
        });

        $(document).on('keypress', function (event) {
            if (event.keyCode == '13') {
                event.preventDefault();
                createDashboardWithInput();
            }
        });

        $('#cancel-dbbtn, .popupOverlay').on('click', function () {
            $('#db-name').val('');
            $('#db-description').val('');
            $('.popupOverlay, .popupContent').removeClass('active');
            $('.error-tip').removeClass('active');
        });
    }

    // Attach event handlers initially
    attachEventHandlers();
}

class btnRenderer {
    init(params) {
        const starOutlineURL = 'url("../assets/star-outline.svg")';
        const starFilledURL = 'url("../assets/star-filled.svg")';

        this.eGui = document.createElement('span');
        this.eGui.innerHTML = `<div id="dashboard-grid-btn" style="margin-left: 20px;">
                <button class="btn-simple" id="delbutton" title="Delete dashboard"></button>
                <button class="btn-duplicate" id="duplicateButton" title="Duplicate dashboard"></button>
                <button class="star-icon" id="favbutton" title="Mark as favorite"></button>
            </div>`;

        this.dButton = this.eGui.querySelector('.btn-simple');
        this.dButton.style.marginRight = '5px';
        this.duplicateButton = this.eGui.querySelector('.btn-duplicate');
        this.starIcon = this.eGui.querySelector('.star-icon');
        this.starIcon.style.backgroundImage = favoriteDBsSet.has(params.data.uniqId) ? starFilledURL : starOutlineURL;

        //Disable delete for default dashboards and show "Default" label
        if (defaultDashboardIds.includes(params.data.uniqId)) {
            const defaultLabel = document.createElement('span');
            defaultLabel.className = 'default-label';
            defaultLabel.innerText = 'Default';
            defaultLabel.style.textDecoration = 'none';
            this.dButton.style.display = 'none';
            this.duplicateButton.parentNode.insertBefore(defaultLabel, this.duplicateButton);
        }

        function deletedb() {
            $.ajax({
                method: 'get',
                url: 'api/dashboards/delete/' + params.data.uniqId,
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
            }).then(function () {
                let deletedRowID = params.data.rowId;
                dbgridOptions.api.applyTransaction({
                    remove: [{ rowId: deletedRowID }],
                });
            });
        }

        function duplicatedb() {
            $.ajax({
                method: 'get',
                url: 'api/dashboards/' + params.data.uniqId,
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
                dataType: 'json',
            }).then(function (res) {
                let duplicatedDBName = res.name + '-Copy';
                let duplicatedDescription = res.description;
                let duplicatedPanels = res.panels;
                let duplicateTimeRange = res.timeRange;
                let duplicateRefresh = res.refresh;
                let uniqIDdb;
                $.ajax({
                    method: 'post',
                    url: 'api/dashboards/create',
                    headers: {
                        'Content-Type': 'application/json; charset=utf-8',
                        Accept: '*/*',
                    },
                    data: JSON.stringify(duplicatedDBName),
                    dataType: 'json',
                    crossDomain: true,
                })
                    .then((res) => {
                        uniqIDdb = Object.keys(res)[0];
                        $.ajax({
                            method: 'POST',
                            url: '/api/dashboards/update',
                            data: JSON.stringify({
                                id: uniqIDdb,
                                name: duplicatedDBName,
                                details: {
                                    name: duplicatedDBName,
                                    description: duplicatedDescription,
                                    panels: duplicatedPanels.map((panel) => ({
                                        ...panel,
                                        style: {
                                            display: panel.style?.display || 'Line chart',
                                            color: panel.style?.color || 'Classic',
                                            lineStyle: panel.style?.lineStyle || 'Solid',
                                            lineStroke: panel.style?.lineStroke || 'Normal',
                                        },
                                    })),
                                    timeRange: duplicateTimeRange,
                                    refresh: duplicateRefresh,
                                },
                            }),
                        });
                    })
                    .then(function () {
                        dbgridOptions.api.applyTransaction({
                            add: [
                                {
                                    dbname: duplicatedDBName,
                                    uniqId: uniqIDdb,
                                },
                            ],
                        });
                    });
            });
        }

        function toggleFavorite() {
            $.ajax({
                method: 'put',
                url: 'api/dashboards/favorite/' + params.data.uniqId,
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
            }).then((response) => {
                // Update the favorite status based on the response
                params.data.favorite = response.isFavorite;
                if (params.data.favorite) {
                    this.starIcon.style.backgroundImage = starFilledURL;
                } else {
                    this.starIcon.style.backgroundImage = starOutlineURL;
                }
            });
        }

        function showPrompt() {
            $('#delete-db-prompt').css('display', 'flex');
            $('.popupOverlay, .popupContent').addClass('active');
            $('#new-dashboard-modal').hide();

            $('#cancel-db-prompt, .popupOverlay').off('click');
            $('#delete-dbbtn').off('click');

            $('#cancel-db-prompt, .popupOverlay').click(function () {
                $('.popupOverlay, .popupContent').removeClass('active');
                $('#delete-db-prompt').hide();
            });

            $('#delete-dbbtn').click(function () {
                deletedb();
                $('.popupOverlay, .popupContent').removeClass('active');
                $('#delete-db-prompt').hide();
            });
        }

        this.dButton.addEventListener('click', showPrompt);
        this.duplicateButton.addEventListener('click', duplicatedb);
        this.starIcon.addEventListener('click', toggleFavorite.bind(this));
    }

    getGui() {
        return this.eGui;
    }

    refresh(params) {
        // Use the URL of the SVG files for star icons
        const starOutlineURL = 'url("../assets/star-outline.svg")';
        const starFilledURL = 'url("../assets/star-filled.svg")';

        this.starIcon.style.backgroundImage = params.data.favorite ? starFilledURL : starOutlineURL;
        return false;
    }
}

let dashboardColumnDefs = [
    {
        field: 'rowId',
        hide: true,
    },
    {
        headerName: 'Dashboard Name',
        field: 'dbname',
        sortable: true,
        cellClass: '',
        cellRenderer: (params) => {
            var link = document.createElement('a');
            link.href = '#';
            link.innerText = params.value;
            link.addEventListener('click', (e) => {
                e.preventDefault();
                view();
            });
            return link;

            function view() {
                $.ajax({
                    method: 'get',
                    url: 'api/dashboards/' + params.data.uniqId,
                    headers: {
                        'Content-Type': 'application/json; charset=utf-8',
                        Accept: '*/*',
                    },
                    crossDomain: true,
                    dataType: 'json',
                }).then(function (_res) {
                    var queryString = '?id=' + params.data.uniqId;
                    window.location.href = '../dashboard.html' + queryString;
                });
            }
        },
    },
    {
        cellRenderer: btnRenderer,
        width: 40,
    },
];

// let the grid know which columns and what data to use
const dbgridOptions = {
    columnDefs: dashboardColumnDefs,
    rowData: dbRowData,
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
    onGridReady(params) {
        this.gridApi = params.api; // To access the grids API
    },
};

function displayDashboards(res, flag) {
    let favorites = [];
    let nonFavorites = [];

    for (let [key, value] of Object.entries(res)) {
        if (favoriteDBsSet.has(key)) {
            favorites.push([key, value]);
        } else {
            nonFavorites.push([key, value]);
        }
    }
    favorites.sort((a, b) => b[1].localeCompare(a[1]));
    nonFavorites.sort((a, b) => b[1].localeCompare(a[1]));
    let resArray = [...favorites, ...nonFavorites];
    res = Object.fromEntries(resArray);
    if (flag == -1) {
        // show search results
        let dbFilteredRowData = [];
        if (dbgridDiv === null) {
            dbgridDiv = document.querySelector('#dashboard-grid');
            //eslint-disable-next-line no-undef
            new agGrid.Grid(dbgridDiv, dbgridOptions);
        }
        dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
        let idx = 0;
        let newRow = new Map();
        $.each(res, function (key, value) {
            newRow.set('rowId', idx);
            newRow.set('uniqId', key);
            newRow.set('dbname', value);

            dbFilteredRowData = _.concat(dbFilteredRowData, Object.fromEntries(newRow));
            idx = idx + 1;
        });
        dbgridOptions.api.setRowData(dbFilteredRowData);
        dbgridOptions.api.sizeColumnsToFit();
    } else {
        if (dbgridDiv === null) {
            dbgridDiv = document.querySelector('#dashboard-grid');
            //eslint-disable-next-line no-undef
            new agGrid.Grid(dbgridDiv, dbgridOptions);
        }
        dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
        let idx = 0;
        let newRow = new Map();
        $.each(res, function (key, value) {
            newRow.set('rowId', idx);
            newRow.set('uniqId', key);
            newRow.set('dbname', value);

            dbRowData = _.concat(dbRowData, Object.fromEntries(newRow));
            idx = idx + 1;
        });
        dbgridOptions.api.setRowData(dbRowData);
        dbgridOptions.api.sizeColumnsToFit();
    }
}

function searchDB() {
    let searchText = $('.search-db-input').val();
    var tokens = searchText
        .toLowerCase()
        .split(' ')
        .filter(function (token) {
            return token.trim() !== '';
        });

    let dbNames = [];
    dbRowData.forEach((rowData) => {
        dbNames.push(rowData.dbname);
    });

    let dbFilteredRowsObject = {};
    if (tokens.length) {
        var searchTermRegex = new RegExp(tokens.join('|'), 'gi');
        dbNames.filter(function (dbName, i) {
            if (dbName.match(searchTermRegex)) {
                let uniqIdDB = dbRowData[i].uniqId;
                dbFilteredRowsObject[`${uniqIdDB}`] = dbRowData[i].dbname;
            }
            return dbName.match(searchTermRegex);
        });

        if (Object.keys(dbFilteredRowsObject).length === 0) {
            displayDashboards(dbFilteredRowsObject, -1);
            showDBNotFoundMsg();
        } else {
            $('#dashboard-grid-container').show();
            $('#empty-response').hide();
            displayDashboards(dbFilteredRowsObject, -1);
        }
    }
}

function displayOriginalDashboards() {
    let searchText = $('.search-db-input').val();

    if (searchText.length === 0) {
        if (dbgridDiv === null) {
            dbgridDiv = document.querySelector('#dashboard-grid');
            //eslint-disable-next-line no-undef
            new agGrid.Grid(dbgridDiv, dbgridOptions);
        }
        $('#dashboard-grid-container').show();
        $('#empty-response').hide();
        dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
        dbgridOptions.api.setRowData(dbRowData);
        dbgridOptions.api.sizeColumnsToFit();
    }
}

function showDBNotFoundMsg() {
    $('#dashboard-grid-container').hide();
    $('#empty-response').show();
}
let favoriteDBsSet;

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);

    let normalDBs = await getAllDashboards();
    let allDefaultDBs = await getAllDefaultDashboards();
    let allDBs = { ...normalDBs, ...allDefaultDBs };
    let favoriteDBs = await getAllFavoriteDashboards();
    // Convert the array of favorite dashboards to a Set for faster lookup
    favoriteDBsSet = new Set(Object.keys(favoriteDBs));
    displayDashboards(allDBs);

    $('#create-db-btn').click(createDashboard);
    $('#run-search').click(searchDB);
    $('.search-db-input').on('keyup', displayOriginalDashboards);

    let stDate = 'now-1h';
    let endDate = 'now';
    datePickerHandler(stDate, endDate, stDate);
});
