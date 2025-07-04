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

let sqgridDiv = null;
let sqRowData = [];
//eslint-disable-next-line no-unused-vars
function setSaveQueriesDialog() {
    let dialog = null;
    let form = null;
    let qname = $('#qname');
    let description = $('#description');
    let allFields = $([]).add(qname).add(description);
    let tips = $('.validateTips');

    function updateTips(t) {
        tips.addClass('active');
        $('.validateTips').show();
        tips.text(t).addClass('ui-state-highlight');
    }

    function checkLength(o, n, min, max) {
        if (o.val().length > max || o.val().length < min) {
            o.addClass('ui-state-error');
            updateTips('Length of ' + n + ' must be between ' + min + ' and ' + max + '.');
            return false;
        } else {
            return true;
        }
    }

    function checkRegexp(o, regexp, n) {
        if (!regexp.test(o.val())) {
            o.addClass('ui-state-error');
            updateTips(n);
            return false;
        } else {
            return true;
        }
    }

    function saveQuery() {
        let valid = true;
        allFields.removeClass('ui-state-error');
        tips.removeClass('ui-state-highlight');
        tips.text('');
        valid = valid && checkLength(qname, 'query name', 3, 30);
        valid = valid && checkRegexp(qname, /^[a-zA-Z0-9_-]+$/i, 'queryname may consist of a-z, 0-9, dash, underscores.');

        if (valid) {
            let data;
            //eslint-disable-next-line no-undef
            if (isMetricsScreen) {
                data = getMetricsDataForSave(qname.val(), description.val());
            } else {
                data = getSearchFilterForSave(qname.val(), description.val());
            }
            //post to save api
            $.ajax({
                method: 'post',
                url: 'api/usersavedqueries/save',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
                dataType: 'text',
                data: JSON.stringify(data),
            })
                .then(function () {
                    dialog.dialog('close');
                    showToast('Query saved successfully', 'success');
                })
                .catch(function (err) {
                    if (err.status !== 200) {
                        showError(`Message: ${err.statusText}`);
                    }
                    dialog.dialog('close');
                });
        }
    }

    dialog = $('#save-queries').dialog({
        autoOpen: false,
        resizable: false,
        maxHeight: 307,
        height: 307,
        width: 464,
        modal: true,
        title: 'Save Query',
        position: {
            my: 'center',
            at: 'center',
            of: window,
        },
        buttons: {
            Cancel: {
                class: 'cancelqButton btn btn-secondary',
                text: 'Cancel',
                click: function () {
                    dialog.dialog('close');
                    hideTooltip();
                },
            },
            Save: {
                class: 'saveqButton btn btn-primary',
                text: 'Save',
                click: function () {
                    saveQuery();
                    hideTooltip();
                },
            },
        },
        close: function () {
            form[0].reset();
            allFields.removeClass('ui-state-error');
            hideTooltip();
        },
        create: function () {
            $(this).parent().find('.ui-dialog-titlebar').show().addClass('border-bottom p-4');
        },
    });

    form = dialog.find('form').on('submit', function (event) {
        event.preventDefault();
        saveQuery();
    });

    $('#saveq-btn').on('click', function () {
        $('.validateTips').hide();
        $('#save-queries').dialog('open');
        $('.ui-widget-overlay').addClass('opacity-75');
        return false;
    });
}

function hideTooltip() {
    const tooltipInstance = $('#saveq-btn')[0]?._tippy;
    if (tooltipInstance) {
        tooltipInstance.hide();
    }
}

//eslint-disable-next-line no-unused-vars
function getSavedQueries() {
    $.ajax({
        method: 'get',
        url: 'api/usersavedqueries/getall',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    }).then(displaySavedQueries);
}

class linkCellRenderer {
    // init method gets the details of the cell to be renderer
    init(params) {
        this.eGui = document.createElement('span');
        let href;
        if (params.data.dataSource === 'metrics') {
            let href = 'metrics-explorer.html?queryString=' + encodeURIComponent(params.data.metricsQueryParams);
            this.eGui.innerHTML = '<a class="query-link" href="' + href + '" title="' + params.data.description + '" style="display:block;">' + params.data.qname + '</a>';
        } else {
            href = 'index.html?searchText=' + encodeURIComponent(params.data.searchText) + '&startEpoch=' + encodeURIComponent(params.data.startTime) + '&endEpoch=' + encodeURIComponent(params.data.endTime) + '&indexName=' + encodeURIComponent(params.data.indexName) + '&filterTab=' + encodeURIComponent(params.data.filterTab) + '&queryLanguage=' + encodeURIComponent(params.data.queryLanguage);
            this.eGui.innerHTML = '<a class="query-link" href=' + href + '" title="' + params.data.description + '"style="display:block;">' + params.data.qname + '</a>';
        }
    }

    getGui() {
        return this.eGui;
    }
    refresh() {
        return false;
    }
}

class btnCellRenderer {
    // init method gets the details of the cell to be renderer
    init(params) {
        this.eGui = document.createElement('div');
        this.eGui.innerHTML = `
        <div id="alert-grid-btn">
            <input type="button" class="btn-simple" id="delbutton"  />
        </div>`;

        // get references to the elements we want
        this.eButton = this.eGui.querySelector('.btn-simple');
        this.eventListener = () => {
            $('.popupOverlay, .popupContent').addClass('active');
            $('#delete-btn').data('params', params);
        };
        this.eButton.addEventListener('click', this.eventListener);
    }

    getGui() {
        return this.eGui;
    }

    // gets called when the cell is removed from the grid
    destroy() {
        // do cleanup, remove event listener from button
        if (this.eButton) {
            // check that the button element exists as destroy() can be called before getGui()
            this.eButton.removeEventListener('click', this.eventListener);
        }
    }

    refresh() {
        return false;
    }
}

// Delete confirmation popup
$(document).ready(function () {
    var currentPage = window.location.pathname;
    if (currentPage.startsWith('/metrics-explorer.html')) {
        //eslint-disable-next-line no-undef
        isMetricsScreen = true;
    }
    $('#cancel-btn, .popupOverlay, #delete-btn').click(function () {
        $('.popupOverlay, .popupContent').removeClass('active');
    });

    // delete function
    $('#delete-btn').click(function () {
        let params = $('#delete-btn').data('params');
        $.ajax({
            method: 'get',
            url: 'api/usersavedqueries/deleteone/' + params.data.qname,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
        }).then(function () {
            let deletedRowID = params.data.rowId;
            sqgridOptions.api.applyTransaction({
                remove: [{ rowId: deletedRowID }],
            });
        });
    });

    $(document).on('keydown', (event) => {
        if (event.key === 'Escape') {
            $('.popupOverlay, .popupContent').removeClass('active');
        }
    });

    $('#sq-filter-input').keyup(function (event) {
        if (event.keyCode == '13') {
            searchSavedQueryHandler(event);
        } else {
            displayOriginalSavedQueries();
        }
    });
    $('#sq-filter-input').on('input', searchSavedQueryHandler);
});

let queriesColumnDefs = [
    {
        field: 'rowId',
        hide: true,
    },
    {
        field: 'qname',
        headerName: 'Query Name',
        cellRenderer: linkCellRenderer,
        resizable: true,
    },
    {
        field: 'qdescription',
        headerName: 'Description',
        resizable: true,
    },
    {
        field: 'type',
        headerName: 'Type',
        resizable: true,
        valueFormatter: (params) => {
            if (params.value) {
                return params.value.charAt(0).toUpperCase() + params.value.slice(1).toLowerCase();
            } else {
                return '';
            }
        },
    },
    {
        field: 'queryLanguage',
        headerName: 'Query Language',
        resizable: true,
    },
    {
        field: 'filterTab',
        headerName: 'FilterTab',
        hide: true,
    },
    {
        field: 'qdelete',
        headerName: 'Delete',
        cellRenderer: btnCellRenderer,
        resizable: false,
    },
];

// let the grid know which columns and what data to use
const sqgridOptions = {
    columnDefs: queriesColumnDefs,
    rowData: sqRowData,
    animateRows: true,
    headerHeight: 26,
    rowHeight: 34,
    defaultColDef: {
        initialWidth: 200,
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        cellClass: 'align-center-grid',
        resizable: true,
        sortable: true,
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    getRowId: (params) => params.data.rowId,
    onGridReady(params) {
        this.gridApi = params.api; // To access the grids API
    },
    localeText: {
        noRowsToShow: 'No Saved Query Found',
    },
    suppressDragLeaveHidesColumns: true,
};

function displaySavedQueries(res, flag) {
    // loop through res and add data to savedQueries
    if (flag === -1) {
        //for search
        let sqFilteredRowData = [];
        if (sqgridDiv === null) {
            sqgridDiv = document.querySelector('#ag-grid');
            //eslint-disable-next-line no-undef
            new agGrid.Grid(sqgridDiv, sqgridOptions);
        }
        sqgridOptions.api.setColumnDefs(queriesColumnDefs);
        let idx = 0;
        let newRow = new Map();
        $.each(res, function (key, value) {
            newRow.set('rowId', idx);
            newRow.set('qdescription', res[key].description);
            newRow.set('searchText', value.searchText);
            newRow.set('indexName', value.indexName);
            newRow.set('type', res[key].dataSource);
            if (res[key].dataSource === 'metrics') {
                newRow.set('queryLanguage', 'PromQL');
            } else {
                newRow.set('queryLanguage', res[key].queryLanguage);
            }
            newRow.set('qname', key);
            newRow.set('filterTab', value.filterTab);
            newRow.set('dataSource', value.dataSource);
            newRow.set('metricsQueryParams', value.metricsQueryParams);
            newRow.set('start', value.startTime);
            newRow.set('end', value.endTime);

            sqFilteredRowData = _.concat(sqFilteredRowData, Object.fromEntries(newRow));
            idx = idx + 1;
        });
        sqgridOptions.api.setRowData(sqFilteredRowData);
        sqgridOptions.api.sizeColumnsToFit();
    } else {
        if (sqgridDiv === null) {
            sqgridDiv = document.querySelector('#ag-grid');
            //eslint-disable-next-line no-undef
            new agGrid.Grid(sqgridDiv, sqgridOptions);
        }
        sqgridOptions.api.setColumnDefs(queriesColumnDefs);
        let idx = 0;
        let newRow = new Map();
        $.each(res, function (key, value) {
            newRow.set('rowId', idx);
            newRow.set('qdescription', res[key].description);
            newRow.set('searchText', value.searchText);
            newRow.set('indexName', value.indexName);
            newRow.set('qname', key);
            if (value.dataSource === 'metrics') {
                newRow.set('queryLanguage', 'PromQL');
            } else {
                newRow.set('queryLanguage', value.queryLanguage);
            }
            newRow.set('filterTab', value.filterTab);
            newRow.set('type', value.dataSource);
            newRow.set('dataSource', value.dataSource);
            newRow.set('metricsQueryParams', value.metricsQueryParams);
            newRow.set('startTime', value.startTime);
            newRow.set('endTime', value.endTime);
            sqRowData = _.concat(sqRowData, Object.fromEntries(newRow));
            idx = idx + 1;
        });
        sqgridOptions.api.setRowData(sqRowData);
        sqgridOptions.api.sizeColumnsToFit();
    }
}

function displayOriginalSavedQueries() {
    let searchText = $('#sq-filter-input').val();
    if (searchText.length === 0) {
        if (sqgridDiv === null) {
            sqgridDiv = document.querySelector('#ag-grid');
            //eslint-disable-next-line no-undef
            new agGrid.Grid(sqgridDiv, sqgridOptions);
        }
        $('#queries-grid-container').show();
        $('#empty-qsearch-response').hide();
        sqgridOptions.api.setColumnDefs(queriesColumnDefs);
        sqgridOptions.api.setRowData(sqRowData);
        sqgridOptions.api.sizeColumnsToFit();
    }
}

function searchSavedQueryHandler(evt) {
    evt.preventDefault();
    $('#empty-qsearch-response').hide();
    let searchText = $('#sq-filter-input').val();
    if (searchText === '') {
        return;
    } else {
        getSearchedQuery();
    }
}

function getSearchedQuery() {
    let searchText = $('#sq-filter-input').val();
    $.ajax({
        method: 'get',
        url: 'api/usersavedqueries/' + searchText,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    })
        .then((res) => {
            $('#queries-grid-container').show();
            displaySavedQueries(res, -1);
        })
        .catch(function (err) {
            if (err.status !== 200) {
                showError(`Message: ${err.statusText}`);
            }
            $('#queries-grid-container').hide();
            let el = $('#empty-qsearch-response');
            el.empty();
            el.append('<span>Query not found.</span>');
            el.show();
        });
}
