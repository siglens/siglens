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
            method: 'GET',
            success: function () {
                $('#ag-grid').empty();
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
        url: '/api/pqs/',
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
    const columnDefs = [
        { headerName: 'Category', field: 'category', sortable: true, filter: true },
        { headerName: 'ID', field: 'id', sortable: true, filter: true },
        { headerName: 'Count', field: 'count', sortable: true, filter: true },
    ];

    const rowData = [];

    // Add promoted_aggregations
    for (const [id, count] of Object.entries(data.promoted_aggregations)) {
        rowData.push({ category: 'Promoted Aggregations', id: id, count: count });
    }

    // Add promoted_searches
    for (const [id, count] of Object.entries(data.promoted_searches)) {
        rowData.push({ category: 'Promoted Searches', id: id, count: count });
    }

    // Add total_tracked_queries
    rowData.push({ category: 'Total Tracked Queries', id: '', count: data.total_tracked_queries });

    const gridOptions = {
        columnDefs: columnDefs,
        rowData: rowData,
        onRowClicked: function (event) {
            if (event.data.id) {
                fetchDetails(event.data.id);
            }
        },
        defaultColDef: {
            flex: 1,
            minWidth: 100,
            resizable: true,
            cellClass: 'align-center-grid',
        },
        headerHeight: 32,
        rowHeight: 42,
    };

    $('#ag-grid').empty();

    const eGridDiv = document.querySelector('#ag-grid');
    //eslint-disable-next-line no-undef
    new agGrid.Grid(eGridDiv, gridOptions);
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
}

$('#close-popup').on('click', function () {
    $('.popupOverlay, #pqs-id-details').removeClass('active');
});
