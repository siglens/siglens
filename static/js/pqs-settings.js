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
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

$(document).ready(function () {
    $('.theme-btn').on('click', themePickerHandler);
    getPersistentQueriesSetting();
    getPQSQueries();
});

$(document).on('click', '.contact-option', updatePQS);

function getPersistentQueriesSetting() {
    $.ajax({
        method: 'GET',
        url: '/api/pqs/get',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
        success: function (res) {
            console.log('Update successful:', res);
            setPersistentQueries(res.pqsEnabled);
        },
        error: function (xhr, status, error) {
            console.error('Update failed:', xhr, status, error);
        },
    });
}

function updatePersistentQueriesSetting(pqsEnabled) {
    $.ajax({
        method: 'POST',
        url: '/api/pqs/update',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
        data: JSON.stringify({ pqsEnabled: pqsEnabled }),
        success: function () {
            showToast('Update successful', 'success');
        },
        error: function () {
            showToast('Update failed', 'error');
        },
    });
}

function updatePQS() {
    var selectedOption = $(this).text();
    $('.contact-option').removeClass('active');

    if (selectedOption.toLowerCase() === 'disabled') {
        $('.popupOverlay, #disable-pqs-prompt').addClass('active');
        $('#cancel-disable-pqs').on('click', function () {
            $('.popupOverlay, #disable-pqs-prompt').removeClass('active');
            $(`.contact-option:contains("Enabled")`).addClass('active');
        });

        $('#disable-pqs').on('click', function () {
            $('#contact-types span').text(selectedOption);
            $('.popupOverlay, #disable-pqs-prompt').removeClass('active');
            $(`.contact-option:contains("Disabled")`).addClass('active');
            updatePersistentQueriesSetting(false);
        });
    }
    if (selectedOption.toLowerCase() === 'enabled') {
        updatePersistentQueriesSetting(true);
        $('#contact-types span').text(selectedOption);
        $(`.contact-option:contains("Enabled")`).addClass('active');
    }
}

function setPersistentQueries(pqsEnabled) {
    $('.contact-option').removeClass('active');
    $('#contact-types span').text(pqsEnabled ? 'Enabled' : 'Disabled');
    $('.contact-option:contains("' + (pqsEnabled ? 'Enabled' : 'Disabled') + '")').addClass('active');
}

$('#clear-pqs-info').on('click', function () {
    $('.popupOverlay, #clear-pqs-prompt').addClass('active');

    $('#clear-pqs').on('click', function () {
        $.ajax({
            url: '/api/pqs/clear',
            method: 'POST',
            success: function () {
                $('.pqs-grid').empty();
                showToast('PQS Info cleared successfully', 'success');
            },
            error: function () {
                showToast("'Error clearing PQS Info", 'error');
            },
        });
        $('.popupOverlay, #clear-pqs-prompt').removeClass('active');
    });

    $('#cancel-pqs').on('click', function () {
        $('.popupOverlay, #clear-pqs-prompt').removeClass('active');
    });
});

function getPQSQueries() {
    $.ajax({
        url: '/api/pqs',
        method: 'GET',
        success: function (response) {
            createTable(response);
        },
        error: function (xhr, status, error) {
            console.error('Error fetching PQS data:', error);
        },
    });
}

function createTable(data) {
    function formatEpochToReadable(epoch) {
        if (!epoch) return '';
        const date = new Date(epoch * 1000);
        return date.toLocaleString();
    }

    const columnDefs = [
        { headerName: 'PQId', field: 'id', sortable: true, filter: true, width: 250, flex: 0 },
        { headerName: 'Count', field: 'count', sortable: true, filter: true, width: 250, flex: 0 },
        {
            headerName: 'Last Used',
            field: 'last_used_epoch',
            sortable: true,
            filter: true,
            width: 250,
            flex: 0,
            valueFormatter: (params) => formatEpochToReadable(params.value),
        },
        { headerName: 'Search Text', field: 'search_text', sortable: true, filter: true, flex: 1, cellClass: 'text-cursor align-center-grid' },
    ];

    const aggregationRowData = data.promoted_aggregations.map((item) => ({
        id: item.id,
        count: item.count,
        last_used_epoch: item.last_used_epoch,
        search_text: item.search_text || '',
    }));

    const aggregationTotalRow = {
        id: 'Total Tracked Aggregations',
        count: aggregationRowData.length,
        last_used_epoch: '',
        search_text: '',
    };

    const aggregationGridOptions = {
        columnDefs: columnDefs,
        rowData: aggregationRowData,
        onCellClicked: function (event) {
            if (event.data.id && event.data.id !== 'Total Tracked Aggregations' && event.colDef.field !== 'search_text') {
                fetchDetails(event.data.id);
            }
        },
        defaultColDef: {
            resizable: true,
            cellClass: 'align-center-grid',
        },
        headerHeight: 26,
        rowHeight: 34,
        pinnedBottomRowData: [aggregationTotalRow],
        suppressDragLeaveHidesColumns: true,
        suppressRowClickSelection: true,
        enableCellTextSelection: true,
    };

    const searchRowData = data.promoted_searches.map((item) => ({
        id: item.id,
        count: item.count,
        last_used_epoch: item.last_used_epoch,
        search_text: item.search_text || '',
    }));

    const searchTotalRow = {
        id: 'Total Tracked Searches',
        count: searchRowData.length,
        last_used_epoch: '',
        search_text: '',
    };

    const searchGridOptions = {
        columnDefs: columnDefs,
        rowData: searchRowData,
        onCellClicked: function (event) {
            if (event.data.id && event.data.id !== 'Total Tracked Searches' && event.colDef.field !== 'search_text') {
                fetchDetails(event.data.id);
            }
        },
        defaultColDef: {
            resizable: true,
            cellClass: 'align-center-grid',
        },
        headerHeight: 26,
        rowHeight: 34,
        pinnedBottomRowData: [searchTotalRow],
        suppressRowClickSelection: true,
        enableCellTextSelection: true,
        suppressDragLeaveHidesColumns: true,
    };

    $('#ag-grid-promoted-aggregations').empty();
    $('#ag-grid-promoted-searches').empty();

    const aggregationGridDiv = document.querySelector('#ag-grid-promoted-aggregations');
    //eslint-disable-next-line no-undef
    new agGrid.Grid(aggregationGridDiv, aggregationGridOptions);

    const searchGridDiv = document.querySelector('#ag-grid-promoted-searches');
    //eslint-disable-next-line no-undef
    new agGrid.Grid(searchGridDiv, searchGridOptions);
}

function fetchDetails(pqid) {
    $.ajax({
        url: `/api/pqs/${pqid}`,
        method: 'GET',
        success: function (response) {
            displayDetails(response);
        },
        error: function (xhr, status, error) {
            console.error('Error fetching details for PQID:', pqid, error);
        },
    });
}

function displayDetails(data) {
    $('.popupOverlay, #pqs-id-details').addClass('active');
    $('#pqs-id-details .header').html(`PQ ID - ${data.pqid}`);
    const formattedJson = JSON.stringify(data, null, 2);
    $('#pqs-id-details .json-body').val(formattedJson);
    $('#pqs-id-details .json-body').scrollTop(0);

    function closePopup() {
        $('.popupOverlay, #pqs-id-details').removeClass('active');
    }

    $('#close-popup, .close-btn').on('click', closePopup);

    // Close popup on Esc key press
    $(document).on('keydown', function (e) {
        if (e.key === 'Escape') {
            closePopup();
        }
    });

    // Remove event listener when popup is closed
    $(document).off('keydown', function (e) {
        if (e.key === 'Escape') {
            closePopup();
        }
    });
}
