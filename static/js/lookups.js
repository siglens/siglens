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

    $('#confirm-delete-btn').on('click', performDelete);
    $('#cancel-delete-btn').on('click', closeDeletePopup);

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
    {
        headerName: 'Action',
        cellRenderer: function (params) {
            return `
                <button class="btn-simple download-button" onclick="downloadLookupFile('${params.data.name}')"></button>
                <button class="btn-simple delete-button mx-4" id="delbutton" onclick="deleteLookupFile('${params.data.name}')"></button>
            `;
        },
    },
];

const gridOptions = {
    columnDefs: columnDefs,
    rowData: [],
    animateRows: true,
    headerHeight: 26,
    rowHeight: 34,
    defaultColDef: {
        cellClass: 'align-center-grid',
        resizable: true,
        sortable: true,
    },
    onRowClicked: function (params) {
        // Check if the click is not on the delete and download button
        if (!$(params.event.target).hasClass('btn-simple')) {
            getLookupFile(params.data.name);
        }
    },
    suppressDragLeaveHidesColumns: true,
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

function uploadFile(overwrite = false) {
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
    formData.append('overwrite', overwrite);

    $.ajax({
        url: '/api/lookup-upload',
        type: 'POST',
        data: formData,
        processData: false,
        contentType: false,
        success: function () {
            showToast('File uploaded successfully.', 'success');
            $('.popupOverlay, #upload-lookup-file, #overwrite-confirmation').removeClass('active');
            $('#db-name').val('');
            $('#file-input').val('');

            // Re-fetch the updated list of lookup files
            fetchLookupFiles();
        },
        error: function (xhr) {
            if (xhr.status === 409 && !overwrite) {
                // File already exists, show confirmation dialog
                $('#existing-file-name').text(name);
                $('.popupOverlay, #overwrite-confirmation').addClass('active');
                $('#upload-lookup-file').removeClass('active');
            } else {
                showToast(`Error uploading file: ${xhr.responseText}`);
            }
        },
    });
}

$('#upload-btn').on('click', function () {
    uploadFile(false);
});

$('#confirm-overwrite-btn').on('click', function () {
    uploadFile(true);
});

$('#cancel-overwrite-btn').on('click', function () {
    $('#overwrite-confirmation').removeClass('active');
    $('#upload-lookup-file').addClass('active');
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

let fileToDelete = '';

//eslint-disable-next-line no-unused-vars
function deleteLookupFile(filename) {
    fileToDelete = filename;
    $('.popupOverlay, #delete-confirmation').addClass('active');
}

//eslint-disable-next-line no-unused-vars
function downloadLookupFile(filename) {
    const downloadUrl = `/api/lookup-files/${encodeURIComponent(filename)}`;
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function performDelete() {
    $.ajax({
        url: `/api/lookup-files/${encodeURIComponent(fileToDelete)}`,
        method: 'DELETE',
        success: function (response) {
            showToast(response, 'success');
            fetchLookupFiles();
            closeDeletePopup();
        },
        error: function (xhr) {
            showToast(`Error deleting file: ${xhr.responseText}`, 'error');
            closeDeletePopup();
        },
    });
}

function closeDeletePopup() {
    $('.popupOverlay, #delete-confirmation').removeClass('active');
    fileToDelete = '';
}

function getLookupFile(filename) {
    $.ajax({
        url: `/api/lookup-files/${encodeURIComponent(filename)}`,
        method: 'GET',
        dataType: 'text',
        success: function (data) {
            if (filename.endsWith('.csv.gz')) {
                displayCompressedFileInfo(filename);
            } else {
                displayCSVContent(filename, data);
            }
        },
        error: function (xhr) {
            showToast(`Error retrieving file: ${xhr.responseText}`, 'error');
        },
    });
}

function displayCompressedFileInfo(filename) {
    $('.popupOverlay, #csvgzViewerModal').addClass('active');
    $('#csvgzViewerModal .header').text(filename);

    const infoHtml = `
    <div class="compressed-file-info">
        <p>This is a compressed file (.csv.gz). You cannot view its contents directly.</p>
    </div>
    <div class="d-flex mt-4">
        <a href="/api/lookup-files/${encodeURIComponent(filename)}" download="${filename}" class="btn btn-primary w-100" >Download
            File</a>
        <button type="button" onclick="closeCSVModal()" class="btn btn-secondary" style="width: 410px; margin-left: 10px;">Close</button>
    </div>
    `;

    $('#csvgzViewerModal .csv-container').html(infoHtml);
}

function displayCSVContent(filename, content) {
    // Split the content into lines
    const lines = content.trim().split('\n');

    // Parse headers
    const headers = lines[0].split(',').map((header) => header.replace(/"/g, '').trim());

    // Parse data
    const rowData = lines.slice(1).map((line) => {
        const values = line.split(',').map((value) => value.replace(/"/g, '').trim());
        return headers.reduce((obj, header, index) => {
            obj[header] = values[index];
            return obj;
        }, {});
    });

    const columnDefs = headers.map((header) => ({
        headerName: header,
        field: header,
        sortable: true,
        filter: true,
    }));

    $('.popupOverlay, #csvViewerModal').addClass('active');
    $('#csvViewerModal .header').text(filename);

    const gridDiv = document.createElement('div');
    gridDiv.id = 'ag-grid';
    gridDiv.style.height = '400px';
    gridDiv.style.width = '100%';
    gridDiv.classList.add('ag-theme-alpine');

    $('#csvViewerModal .csv-container').empty().append(gridDiv);
    $('#csvViewerModal .csv-container').append(`
    <div class="d-flex mt-4 justify-content-end">
        <button type="button" onclick="closeCSVModal()" class="btn btn-secondary" style="width: 150px; margin-left: 10px;">Close</button>
    </div>`);

    //eslint-disable-next-line no-undef
    new agGrid.Grid(gridDiv, {
        columnDefs: columnDefs,
        rowData: rowData,
        defaultColDef: {
            flex: 1,
            minWidth: 100,
            resizable: true,
            sortable: true,
        },
    });
}

//eslint-disable-next-line no-unused-vars
function closeCSVModal() {
    $('.popupOverlay, #csvViewerModal, #csvgzViewerModal').removeClass('active');
}
