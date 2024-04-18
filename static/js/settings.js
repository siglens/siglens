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
    getRetentionDataFromConfig();
    getPersistentQueriesSetting();
    getSystemInfo();
    {{ .SettingsExtraOnReadySetup }}
    {{ .Button1Function }}
});

function getRetentionDataFromConfig() {
    $.ajax({
        method: 'get',
        url: 'api/config',
        crossDomain: true,
        dataType: 'json',
        credentials: 'include'
    })
    {{ if .SettingsRetentionDataThenBlock }}
        {{ .SettingsRetentionDataThenBlock }}
    {{ else }}
        .then((res) => {
            let ret_days = res.RetentionHours / 24;
            $('#retention-days-value').html(`${ret_days} days`);
        })
    {{ end }}
    .catch((err) => {
        console.log(err)
    });
}

function getPersistentQueriesSetting(){
    $.ajax({
        method: "GET",
        url: "/api/pqs/get",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
        crossDomain: true,
        success: function (res) {
            console.log("Update successful:", res);
            setPersistentQueries(res.pqsEnabled);
        },
        error: function (xhr, status, error) {
            console.error("Update failed:", xhr, status, error);
        },
    });
}
function updatePersistentQueriesSetting(pqsEnabled) {
    $.ajax({
        method: "POST",
        url: "/api/pqs/update",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
        crossDomain: true,
        data: JSON.stringify({ pqsEnabled: pqsEnabled }),
        success: function (res) {
            console.log("Update successful:", res);
        },
        error: function (xhr, status, error) {
            console.error("Update failed:", xhr, status, error);
        },
    });
}

$(document).on('click', '.contact-option', updatePQS);

function updatePQS() {
    var selectedOption = $(this).text();
    $('.contact-option').removeClass('active');

    if (selectedOption.toLowerCase() === 'disabled') {
        $('.popupOverlay, .popupContent').addClass('active');
        $('#cancel-disable-pqs').on('click', function() {
            $('.popupOverlay, .popupContent').removeClass('active');
            $(`.contact-option:contains("Enabled")`).addClass('active');
        });
        
        $('#disable-pqs').on('click', function() {
            $('#contact-types span').text(selectedOption); 
            $('.popupOverlay, .popupContent').removeClass('active');
            $(`.contact-option:contains("Disabled")`).addClass('active');
            updatePersistentQueriesSetting(false);
        });
    }
    if(selectedOption.toLowerCase() === 'enabled') {
        updatePersistentQueriesSetting(true);
        $('#contact-types span').text(selectedOption); 
        $(`.contact-option:contains("Enabled")`).addClass('active');
    }
}

function setPersistentQueries(pqsEnabled) {
    $('.contact-option').removeClass('active');
    $('#contact-types span').text(pqsEnabled ? "Enabled" : "Disabled");
    $('.contact-option:contains("' + (pqsEnabled ? "Enabled" : "Disabled") + '")').addClass('active');
}

{{ .SettingsExtraFunctions }}

function getSystemInfo(){
    $.ajax({
        method: "GET",
        url: "/api/system-info",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
        crossDomain: true,
        success: function (res) {
            addSystemInfoTable(res);
        },
        error: function (xhr, status, error) {
            console.error("Update failed:", xhr, status, error);
        },
    });
}

function addSystemInfoTable(systemInfo) {
    var table = $("#system-info-table");
    
    function createRow(header, value) {
        return `<tr><th>${header}</th><td>${value}</td></tr>`;
    }

    var osRow = createRow("Operating System", systemInfo.os);
    var cpuRow = createRow("vCPU Count", systemInfo.v_cpu);
    var memoryUsage = systemInfo.memory.used_percent.toFixed(2);
    var totalMemoryGB = (systemInfo.memory.total / Math.pow(1024, 3)).toFixed(2);
    var availableMemoryGB = (systemInfo.memory.free / Math.pow(1024, 3)).toFixed(2);
    var memoryInfo = `<div><b>Total:</b> ${totalMemoryGB} GB</div>
                    <div><b>Available:</b> ${availableMemoryGB} GB</div>
                    <div><b>Used:</b> ${memoryUsage}%</div>`;
    var memoryRow = createRow("Memory Usage", memoryInfo);
    var diskUsage = systemInfo.disk.used_percent.toFixed(2);
    var totalDiskGB = (systemInfo.disk.total / Math.pow(1024, 3)).toFixed(2);
    var availableDiskGB = (systemInfo.disk.free / Math.pow(1024, 3)).toFixed(2);
    var diskInfo = `<div><b>Total:</b> ${totalDiskGB} GB</div>
                    <div><b>Available:</b> ${availableDiskGB} GB</div>
                    <div><b>Used:</b> ${diskUsage}%</div>`;
    var diskRow = createRow("Disk Usage", diskInfo);
    var uptime = createRow("Process Uptime", formatUptime(systemInfo.uptime));

    table.append(uptime, cpuRow, memoryRow, osRow, diskRow);
}

function formatUptime(uptimeMinutes) {
    if (uptimeMinutes < 60) {
        return uptimeMinutes + " mins";
    } else if (uptimeMinutes < 24*60) {
        return Math.floor(uptimeMinutes / 60) + " hours";
    } else if (uptimeMinutes < 7*24*60) {
        return Math.floor(uptimeMinutes / (24*60)) + " days";
    } else if (uptimeMinutes < 30*24*60) {
        return Math.floor(uptimeMinutes / (7*24*60)) + " weeks";
    } else {
        return Math.floor(uptimeMinutes / (30*24*60)) + " months";
    }
}

