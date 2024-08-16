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

$(document).ready(function () {
    $('.theme-btn').on('click', themePickerHandler);
    fetchLookupFiles();
});

function fetchLookupFiles() {
    $.ajax({
        url: '/api/lookup-files',
        method: 'GET',
        dataType: 'json',
        success: function (data) {
            displayLookupFiles(data);
        },
        error: function (xhr, status, error) {
            console.error('Failed to fetch lookup files:', error);
        },
    });
}

const columnDefs = [
    { headerName: 'Name', field: 'name', resizable: true, sortable: true },
    { headerName: 'Destination', field: 'destination', resizable: true, sortable: true },
];

const gridOptions = {
    columnDefs: columnDefs,
    rowData: [],
    animateRows: true,
    rowHeight: 44,
    headerHeight: 32,
    defaultColDef: {
        cellClass: 'align-center-grid',
        resizable: true,
        sortable: true,
    },
};
let eGridDiv = $('#ag-grid')[0];
//eslint-disable-next-line no-undef
new agGrid.Grid(eGridDiv, gridOptions);

function displayLookupFiles(data) {
    const rowData = data.map((fileName) => {
        return {
            name: fileName,
            destination: 'data/lookups/' + fileName,
        };
    });
    gridOptions.api.setRowData(rowData);
    gridOptions.api.sizeColumnsToFit();
}

$('#upload-btn').on('click', function () {
    const name = $('#db-name').val();
    const fileInput = $('#file-input')[0];

    if (!name) {
        $('.name-empty').addClass('active');
        return;
    }

    if (!fileInput.files.length) {
        $('.file-empty').addClass('active');
        return;
    }

    const file = fileInput.files[0];
    const formData = new FormData();
    formData.append('name', name);
    formData.append('file', file);

    $.ajax({
        url: '/api/lookup-upload',
        type: 'POST',
        data: formData,
        processData: false,
        contentType: false,
        success: function () {
            showToast('File uploaded successfully.', 'success');
            $('.popupOverlay, #upload-lookup-file').removeClass('active');
            $('#db-name').val('');
            $('#file-input').val('');

            // Re-fetch the updated list of lookup files
            fetchLookupFiles();
        },
        error: function (xhr) {
            showToast(`Error uploading file: ${xhr.responseText}`);
        },
    });
});

$('#new-lookup-file').on('click', function () {
    $('.popupOverlay, #upload-lookup-file').addClass('active');
});

$('#cancel-upload-btn').on('click', function () {
    $('.popupOverlay, #upload-lookup-file').removeClass('active');
    $('#db-name').val('');
    $('#file-input').val('');
});

$('#db-name').focus(() => $('.name-empty').removeClass('active'));
$('#file-input').focus(() => $('.file-empty').removeClass('active'));
