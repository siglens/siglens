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
var progressBar = $('#progressbar');
var progressLabel = $('.progress-label');
let confirmDownload = true;
$(document).ready(() => {
    setDownloadLogsDialog();
});
$(function () {
    if (typeof interval != 'undefined') {
        doClearInterval();
    } else {
        interval = null;
    }
});

var progressWidth = 0;
function beginProgress(t) {
    progressWidth = 1;
    interval = setInterval(doProgress, t * 10);
}
function cancelDownload() {
    confirmDownload = false;
    var popBox = document.getElementById('pop-box');
    var popLayer = document.getElementById('pop-layer');
    popLayer.style.display = 'none';
    popBox.style.display = 'none';
    $('#progressbar').hide();
}
function setProgress(node, width) {
    if (node) {
        progressBar.progressbar({
            value: width,
        });
        progressLabel.text(width + '%');
    }
}
function doProgress() {
    if (progressWidth == 98) {
        doClearInterval();
    }
    setProgress(progressBar, progressWidth);
    progressWidth++;
}
function doClearInterval() {
    clearInterval(interval);
}

function setDownloadLogsDialog() {
    let dialog = null;
    let form = null;
    let qname = $('#qnameDL');
    let description = $('#descriptionR');
    let allFields = $([]).add(qname).add(description);
    let tips = $('#validateTips');

    function updateTips(t) {
        tips.addClass('active');
        $('#validateTips').show();
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

    // Function to convert JSON data to XML format
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

    // Function to download XML data as a file
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

    // Function to convert JSON data to SQL format
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

    // Function to download SQL data as a file
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
            $('#progressbar').show();
            //show progress box
            var popBox = document.getElementById('pop-box');
            var popLayer = document.getElementById('pop-layer');
            popLayer.style.width = document.body.scrollWidth + 'px';
            popLayer.style.height = document.body.scrollHeight + 'px';
            popLayer.style.display = 'block';
            popBox.style.display = 'block';

            let params = getSearchFilter(false, false);
            let searchText = params.searchText;
            let n = searchText.indexOf('BY');
            let textArr = [];
            if (n != -1) {
                let textCut = searchText.substring(n + 2, searchText.length);
                let arrNew = textCut.split(',');
                for (let i = 0; i < arrNew.length; i++) {
                    arrNew[i] = arrNew[i].trim();
                }
                textArr = arrNew.sort();
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
                    //close progress box
                    var popBox = document.getElementById('pop-box');
                    var popLayer = document.getElementById('pop-layer');
                    popLayer.style.display = 'none';
                    popBox.style.display = 'none';
                    //set progress finished
                    doClearInterval();
                    $('#progressbar').hide();
                    setProgress(progressBar, 100);
                    if (!confirmDownload) return;
                    if (res && res.hits && res.hits.records && res.hits.records.length > 0 && res.qtype === 'logs-query') {
                        let json = JSON.stringify(res.hits.records);
                        downloadData(json, name);
                    } else if (res && res.qtype === 'aggs-query' && res.aggregations && res.aggregations[''].buckets.length > 0) {
                        let arr = res.aggregations[''].buckets;
                        let createNewRecords = [];
                        for (let i = 0; i < arr.length; i++) {
                            let perInfo = arr[i];
                            let newPerInfo = {};
                            for (let key in perInfo) {
                                if (key != 'key' && key != 'doc_count') newPerInfo[key] = perInfo[key].value;
                                else if (key == 'key') {
                                    for (let j = 0; j < textArr.length; j++) {
                                        if (Array.isArray(perInfo.key)) {
                                            newPerInfo[textArr[j]] = perInfo.key[j];
                                        } else {
                                            newPerInfo[textArr[j]] = perInfo.key;
                                        }
                                    }
                                }
                            }
                            createNewRecords.push(newPerInfo);
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
                        alert('no data available');
                    }
                },
                error: function () {
                    doClearInterval();
                },
            });
        }
    }
    dialog = $('#download-info').dialog({
        autoOpen: false,
        resizable: false,
        width: 460,
        modal: true,
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
                text: 'Save',
                click: download,
            },
        },
        close: function () {
            form[0].reset();
            allFields.removeClass('ui-state-error');
        },
    });

    form = dialog.find('form').on('submit', function (event) {
        event.preventDefault();
        download();
    });
    $('#csv-block').on('click', function () {
        curChoose = '.csv';
        $('#validateTips').hide();
        $('#download-info').dialog('open');
        $('.ui-widget-overlay').addClass('opacity-75');
        return false;
    });
    $('#json-block').on('click', function () {
        curChoose = '.json';
        $('#validateTips').hide();
        $('#download-info').dialog('open');
        $('.ui-widget-overlay').addClass('opacity-75');
        return false;
    });
    $('#xml-block').on('click', function () {
        curChoose = '.xml';
        $('#validateTips').hide();
        $('#download-info').dialog('open');
        $('.ui-widget-overlay').addClass('opacity-75');
        return false;
    });
    $('#sql-block').on('click', function () {
        curChoose = '.sql';
        $('#validateTips').hide();
        $('#download-info').dialog('open');
        $('.ui-widget-overlay').addClass('opacity-75');
        return false;
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

// Delete confirmation popup
$(document).ready(function () {
    $('#cancel-loading').on('click', cancelDownload);
});
