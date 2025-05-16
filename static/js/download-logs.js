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

let curChoose = '';
let interval = null;
let confirmDownload = true;
let progressWidth = 0;

$(document).ready(() => {
    setDownloadLogsDialog();
    $('#cancel-loading').on('click', cancelDownload);
});

function beginProgress(t) {
    progressWidth = 1;
    interval = setInterval(doProgress, t * 10);
}

function cancelDownload() {
    confirmDownload = false;
    $('.popupOverlay, .download-progress-box').removeClass('active');
    clearInterval(interval);
}

function setProgress(width) {
    $('.download-progress-bar').css('width', width + '%');
    $('.download-progress-label').text(width + '%');
}

function doProgress() {
    if (progressWidth >= 98) {
        clearInterval(interval);
    }
    setProgress(progressWidth);
    progressWidth++;
}

function setDownloadLogsDialog() {
    $('body').append(`
        <div class="download-progress-box popupContent p-0">
            <div class="p-4 border-bottom">
                <h3 class="header mb-0">Preparing Your Download</h3>
            </div>
            <div class="p-4 border-bottom">
                <div class="download-progress-bar-container">
                    <div class="download-progress-bar"></div>
                </div>
                <div class="download-progress-label">0%</div>
            </div>
            <div class="p-4 d-flex justify-content-end"><button id="cancel-loading" class="btn btn-secondary">Cancel</button></div>
        </div>
    `);

    $('#download-info').append(`
        <div class="scope-selection mt-4 mb-3 mx-1">
            <label class="form-label">Download Scope:</label>
            <div class="form-check d-flex align-items-end">
                <input class="form-check-input" type="radio" name="downloadScope" id="currentScope" value="current" checked>
                <label class="form-check-label mx-2" for="currentScope">
                    Current View (<span id="current-record-count">0</span> records)
                </label>
            </div>
            <div class="form-check d-flex align-items-end mt-1">
                <input class="form-check-input" type="radio" name="downloadScope" id="allScope" value="all">
                <label class="form-check-label mx-2" for="allScope">
                    All Records (up to 10,000 records)
                </label>
            </div>
        </div>
    `);

    $('#download-info').find('.scope-selection').append(`
        <div id="all-records-warning" class="warning-alert" style="display:none;">
            <i class="fas fa-exclamation-triangle" style="margin-right: 8px; color: #ffc107; font-size:16px;"></i>
            <span>Warning: Downloading all records may take longer and consume more resources.</span>
        </div>
    `);


    let dialog = null;
    let form = null;
    let qname = $('#qnameDL');
    let description = $('#descriptionR');
    let allFields = $([]).add(qname).add(description);
    let tips = $('#validateTips');

    function updateTips(t) {
        tips.addClass('active');
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

    function downloadJson(fileName, json) {
        const jsonStr = json instanceof Object ? JSON.stringify(json) : json;
        const url = window.URL || window.webkitURL || window;
        const blob = new Blob([jsonStr]);
        const saveLink = document.createElementNS('http://www.w3.org/1999/xhtml', 'a');
        saveLink.href = url.createObjectURL(blob);
        saveLink.download = fileName;
        saveLink.click();
    }

    function convertToCSV(json) {
        const items = JSON.parse(json);

        if (!Array.isArray(items)) {
            // Handle single object case
            const headers = Object.keys(items);
            const csvHeader = headers.join(',');
            const csvBody = headers
                .map((header) => {
                    let col = items[header];
                    return typeof col !== 'string' ? col : `"${col.replace(/"/g, '""')}"`;
                })
                .join(',');
            return `${csvHeader}\n${csvBody}`;
        }

        // Get column headers from first item in JSON array
        const headers = Object.keys(items[0]);

        // Build CSV header row
        const csvHeader = headers.join(',');

        // Build CSV body rows
        const csvBody = items
            .map((item) => {
                return headers
                    .map((header) => {
                        let col = item[header];
                        return typeof col !== 'string' ? col : `"${col.replace(/"/g, '""')}"`;
                    })
                    .join(',');
            })
            .join('\n');

        // Combine header and body into single CSV string
        const csv = `${csvHeader}\n${csvBody}`;

        return csv;
    }

    function downloadCsv(csvData, fileName) {
        const blob = new Blob([csvData], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const downloadLink = document.createElement('a');
        downloadLink.href = url;
        downloadLink.download = fileName;
        document.body.appendChild(downloadLink);
        downloadLink.click();
        document.body.removeChild(downloadLink);
    }

    function convertToXML(json) {
        const items = JSON.parse(json);
        let xmlString = '<?xml version="1.0" encoding="UTF-8"?>\n<root>\n';
        if (!Array.isArray(items)) {
            xmlString += '  <item>\n';
            Object.keys(items).forEach((key) => {
                xmlString += `    <${key}>${items[key]}</${key}>\n`;
            });
            xmlString += '  </item>\n';
        } else {
            items.forEach((item) => {
                xmlString += '  <item>\n';
                Object.keys(item).forEach((key) => {
                    xmlString += `    <${key}>${item[key]}</${key}>\n`;
                });
                xmlString += '  </item>\n';
            });
        }
        xmlString += '</root>';
        return xmlString;
    }

    function downloadXml(xmlData, fileName) {
        const blob = new Blob([xmlData], { type: 'text/xml' });
        const url = URL.createObjectURL(blob);
        const downloadLink = document.createElement('a');
        downloadLink.href = url;
        downloadLink.download = fileName;
        document.body.appendChild(downloadLink);
        downloadLink.click();
        document.body.removeChild(downloadLink);
    }

    function convertToSQL(json) {
        const data = JSON.parse(json);
        const tableName = 'SQL_Table';

        if (!Array.isArray(data)) {
            const columns = Object.keys(data);
            const values = columns
                .map((col) => {
                    const value = typeof data[col] === 'string' ? `'${data[col].replace(/'/g, "''")}'` : data[col];
                    return value;
                })
                .join(', ');

            return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values});`;
        }

        // Generate SQL INSERT statements for each object in the data array
        const columns = Object.keys(data[0]);

        const sqlStatements = data.map((item) => {
            const values = columns
                .map((col) => {
                    // Escape single quotes in string values and wrap in quotes
                    const value = typeof item[col] === 'string' ? `'${item[col].replace(/'/g, "''")}'` : item[col];
                    return value;
                })
                .join(', '); // Join column values with commas

            return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values});`;
        });
        return sqlStatements.join('\n');
    }

    function downloadSql(sqlData, fileName) {
        const blob = new Blob([sqlData], { type: 'text/sql' });
        const url = URL.createObjectURL(blob);
        const downloadLink = document.createElement('a');
        downloadLink.href = url;
        downloadLink.download = fileName;
        document.body.appendChild(downloadLink);
        downloadLink.click();
        document.body.removeChild(downloadLink);
    }

    $('input[name="downloadScope"]').on('change', function() {
        if ($(this).val() === 'all') {
          $('#all-records-warning').show();
        } else {
          $('#all-records-warning').hide();
        }
      });

    function download() {
        confirmDownload = true;
        let valid = true;
        allFields.removeClass('ui-state-error');
        tips.removeClass('ui-state-highlight');
        tips.text('');
        valid = valid && checkLength(qname, 'download name', 1, 254);
        valid = valid && checkRegexp(qname, /^[a-zA-Z0-9_.-]+$/i, 'Download name may consist of a-z, 0-9, period, dash, underscores.');
        let enteredName = $('#qnameDL').val();
        let extension = curChoose;
        let name = enteredName;

        if (!enteredName.endsWith(extension)) {
            name += extension;
        }

        if (valid) {
            dialog.dialog('close');

            $('.popupOverlay, .download-progress-box').addClass('active');
            $('.download-progress-bar').css('width', '0%');
            $('.download-progress-label').text('0%');

            const useAllRecords = $('#allScope').is(':checked');

            let params = getSearchFilter(false, false);
            if (useAllRecords) {
                params.size = 10000;
            } else {
                params.size = totalLoadedRecords;
            }
            let searchText = params.searchText;
            let n = searchText.indexOf('BY');
            if (n != -1) {
                let textCut = searchText.substring(n + 2, searchText.length);
                let arrNew = textCut.split(',');
                for (let i = 0; i < arrNew.length; i++) {
                    arrNew[i] = arrNew[i].trim();
                }
            }

            $.ajax({
                method: 'post',
                url: 'api/search',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
                dataType: 'json',
                data: JSON.stringify(params),
                beforeSend: function () {
                    beginProgress(10);
                },
                success: function (res) {
                    $('.popupOverlay, .download-progress-box').removeClass('active');

                    clearInterval(interval);

                    if (!confirmDownload) return;

                    if (res && res.hits && res.hits.records && res.hits.records.length > 0 && res.qtype === 'logs-query') {
                        let json = JSON.stringify(res.hits.records);

                        downloadData(json, name);
                    } else if (res && res.qtype === 'aggs-query' && res.measure && res.measure.length > 0) {
                        let createNewRecords = [];

                        for (let i = 0; i < res.measure.length; i++) {
                            const item = res.measure[i];
                            const newItem = {};

                            if (item.GroupByValues && item.GroupByValues.length > 0) {
                                for (let j = 0; j < item.GroupByValues.length; j++) {
                                    const columnName = res.groupByCols && res.groupByCols[j] ? res.groupByCols[j] : `group_${j}`;
                                    newItem[columnName] = item.GroupByValues[j];
                                }
                            }

                            if (item.MeasureVal) {
                                for (const key in item.MeasureVal) {
                                    newItem[key] = item.MeasureVal[key];
                                }
                            }

                            createNewRecords.push(newItem);
                        }

                        let json = JSON.stringify(createNewRecords);
                        downloadData(json, name);
                    } else if (res.qtype === 'segstats-query') {
                        let segstatsData = {};

                        if (res.measure && res.measure.length > 0 && res.measure[0].MeasureVal) {
                            for (let key in res.measure[0].MeasureVal) {
                                segstatsData[key] = res.measure[0].MeasureVal[key];
                            }
                        }

                        let json = JSON.stringify(segstatsData);
                        downloadData(json, name);
                    } else {
                        alert('No data available');
                    }
                },
                error: function (err) {
                    console.error('Error fetching data:', err);
                    $('.popupOverlay, .download-progress-box').removeClass('active');
                    clearInterval(interval);
                    alert('An error occurred while fetching data. Please try again.');
                },
            });
        }
    }

    dialog = $('#download-info').dialog({
        autoOpen: false,
        resizable: false,
        width: 464,
        modal: true,
        title: 'Download Data',
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
                },
            },
            Save: {
                class: 'saveqButton btn btn-primary',
                text: 'Download',
                click: download,
            },
        },
        open: function () {
            $('#format').val(curChoose.replace('.', '').toUpperCase());

            $('#currentScope').prop('checked', true);
            $('#all-records-warning').hide();

            if (lastQType === 'logs-query' && totalLoadedRecords >= 100 && totalLoadedRecords >= 100) {
                $('.scope-selection').show();
                $('#current-record-count').html(totalLoadedRecords);
            } else {
                $('.scope-selection').hide();
            }
        },
        close: function () {
            form[0].reset();
            allFields.removeClass('ui-state-error');
            $('.download-format-item').removeClass('selected');
        },
        create: function () {
            $(this).parent().find('.ui-dialog-titlebar').show().addClass('border-bottom p-4');
        },
    });

    form = dialog.find('form').on('submit', function (event) {
        event.preventDefault();
        download();
    });

    const downloadOptionToExtension = {
        'csv-block': '.csv',
        'json-block': '.json',
        'xml-block': '.xml',
        'sql-block': '.sql',
    };

    Object.keys(downloadOptionToExtension).forEach((optionId) => {
        $(`#${optionId}`).on('click', function () {
            curChoose = downloadOptionToExtension[optionId];
            $('#validateTips').hide();
            $('#download-info').dialog('open');
            $('.ui-widget-overlay').addClass('opacity-75');
            return false;
        });
    });

    function downloadData(json, fileName) {
        if (curChoose === '.json') {
            downloadJson(fileName, json);
        } else if (curChoose === '.csv') {
            const csvData = convertToCSV(json);
            downloadCsv(csvData, fileName);
        } else if (curChoose === '.xml') {
            const xmlData = convertToXML(json);
            downloadXml(xmlData, fileName);
        } else if (curChoose === '.sql') {
            const sqlData = convertToSQL(json);
            downloadSql(sqlData, fileName);
        }
    }
}
