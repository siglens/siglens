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

{{ .SettingsExtraFunctions }}

async function getSystemInfo() {
    try {
        const res = await $.ajax({
            method: "GET",
            url: "/api/system-info",
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                Accept: "*/*",
            },
            dataType: "json",
            crossDomain: true,
        });

        addSystemInfoTable(res);
        return res;
    } catch (error) {
        console.error("Update failed:", error);
    }
}

function addSystemInfoTable(systemInfo) {
    var table = $("#system-info-table");

    function createRow(header, value) {
        return `<tr><th>${header}</th><td>${value}</td></tr>`;
    }

    var osRow = createRow("Operating System", systemInfo.os);
    var cpuRow = createRow("vCPUs", systemInfo.v_cpu);
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
    if (uptimeMinutes === 0) {
        return "0 minutes";
    }

    const minutes = uptimeMinutes % 60;
    const hours = Math.floor(uptimeMinutes / 60) % 24;
    const days = Math.floor(uptimeMinutes / (24 * 60)) % 7;
    const weeks = Math.floor(uptimeMinutes / (7 * 24 * 60));
    const months = Math.floor(uptimeMinutes / (30 * 24 * 60));

    const formatUnit = (value, unit) => 
        value > 0 ? `${value} ${unit}${value > 1 ? 's' : ''}` : '';

    if (months > 0) {
        const remainingDays = Math.floor((uptimeMinutes % (30 * 24 * 60)) / (24 * 60));
        const remainingWeeks = Math.floor(remainingDays / 7);
        const monthPart = formatUnit(months, 'month');
        if (remainingWeeks > 0) {
            return `${monthPart} ${formatUnit(remainingWeeks, 'week')}`.trim();
        } else if (remainingDays > 0) {
            return `${monthPart} ${formatUnit(remainingDays, 'day')}`.trim();
        }
        return monthPart;
    } else if (weeks > 0) {
        return `${formatUnit(weeks, 'week')} ${formatUnit(days, 'day')}`.trim();
    } else if (days > 0) {
        return `${formatUnit(days, 'day')} ${formatUnit(hours, 'hour')}`.trim();
    } else if (hours > 0) {
        return `${formatUnit(hours, 'hour')} ${formatUnit(minutes, 'minute')}`.trim();
    } else {
        return formatUnit(minutes, 'minute');
    }
}

