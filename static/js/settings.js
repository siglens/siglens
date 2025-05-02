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

let currentTimeout = 0;
let systemInfo, inodeInfo;

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);
    getRetentionDataFromConfig();
    const result = await getSystemAndInodeInfo();
    if (result) {
        systemInfo = result.systemInfo;
        inodeInfo = result.inodeInfo;
    }
    fetchQueryTimeout()

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

async function getSystemAndInodeInfo() {
    try {
        const [systemInfo, inodeInfo] = await Promise.all([
            $.ajax({
                method: "GET",
                url: "/api/system-info",
                headers: {
                    "Content-Type": "application/json; charset=utf-8",
                    Accept: "*/*",
                },
                dataType: "json",
                crossDomain: true,
            }),
            $.ajax({
                method: "GET",
                url: "/api/inode-stats",
                headers: {
                    "Content-Type": "application/json; charset=utf-8",
                    Accept: "*/*",
                },
                dataType: "json",
                crossDomain: true,
            })
        ]);

        addSystemInfoTable(systemInfo, inodeInfo);
        return { systemInfo, inodeInfo };
    } catch (error) {
        console.error("Update failed:", error);
    }
}

function addSystemInfoTable(systemInfo, inodeInfo) {
    var table = $("#system-info-table");
    table.empty();

    function createRow(header, value) {
        return `<tr><th>${header}</th><td>${value}</td></tr>`;
    }

    var osRow = createRow("Operating System", systemInfo.os);
    var cpuRow = createRow("vCPUs", systemInfo.v_cpu);

    var memoryUsage = systemInfo.memory.used_percent.toFixed(2);
    var totalMemory = systemInfo.memory.total;
    var availableMemory = systemInfo.memory.free;

    var totalMemoryGB = (totalMemory / Math.pow(1024, 3)).toFixed(2);
    var availableMemoryGB = (availableMemory / Math.pow(1024, 3)).toFixed(2);
    var totalMemoryMB = (totalMemory / Math.pow(1024, 2)).toFixed(2);
    var availableMemoryMB = (availableMemory / Math.pow(1024, 2)).toFixed(2);

    var totalMemoryDisplay = totalMemoryGB >= 1 
        ? `${totalMemoryGB} GB` 
        : `${totalMemoryMB} MB`;

    var availableMemoryDisplay = availableMemoryGB >= 1 
        ? `${availableMemoryGB} GB` 
        : `${availableMemoryMB} MB`;

    var memoryInfo =`<div class="usage-info">
                    <div><b>Total:</b> ${totalMemoryDisplay}</div>
                    <div><b>Available:</b> ${availableMemoryDisplay}</div>
                    <div><b>Used:</b> ${memoryUsage}%</div>
                    </div>`;

    var memoryRow = createRow("Memory Usage", memoryInfo);

    var diskUsage = systemInfo.disk.used_percent.toFixed(2);
    var totalDiskGB = (systemInfo.disk.total / Math.pow(1024, 3)).toFixed(2);
    var availableDiskGB = (systemInfo.disk.free / Math.pow(1024, 3)).toFixed(2);
    var diskInfo = `<div class="usage-info">
                    <div><b>Total:</b> ${totalDiskGB} GB</div>
                    <div><b>Available:</b> ${availableDiskGB} GB</div>
                    <div><b>Used:</b> ${diskUsage}%</div>
                    </div>`;
    var diskRow = createRow("Disk Usage", diskInfo);

    var totalInodes = inodeInfo.totalInodes.toLocaleString();
    var usedInodes = inodeInfo.usedInodes.toLocaleString();
    var freeInodes = inodeInfo.freeInodes.toLocaleString();
    var inodeInfo = `<div class="usage-info">
                     <div><b>Total:</b> ${totalInodes}</div>
                     <div><b>Available:</b> ${freeInodes}</div>
                     <div><b>Used:</b> ${usedInodes}</div>
                     </div>`;
    var inodeRow = createRow("iNode Usage", inodeInfo);

    var uptime = createRow("Process Uptime", formatUptime(systemInfo.uptime));

    table.append(uptime, cpuRow, memoryRow, osRow, diskRow, inodeRow);
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

 function fetchQueryTimeout() {
    $.ajax({
        url: '/api/get-query-timeout',
        method: 'GET',
        success: function(response) {
            // Convert seconds to minutes
            currentTimeout = Math.floor(response.timeoutSecs / 60);
            $('#queryTimeout').val(currentTimeout);
        },
        error: function() {
            showToast('Failed to load timeout setting', 'error')
        }
    });
}

let timeoutId;
$('#queryTimeout').on('input change', function() {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => {
        let newValue = Math.round(parseFloat($(this).val()));
        
        const isValid = !isNaN(newValue) && newValue >= 1 && newValue <= 30;
        
        if (newValue > 30) {
            newValue = 30;
        } else if (newValue < 1 && newValue !== '') {
            newValue = 1;
        }
        
        $(this).val(newValue);
        
        if (isValid && newValue !== currentTimeout) {
            $('#saveTimeout').show();
        } else {
            $('#saveTimeout').hide();
        }
    }, 50);
});

$('#saveTimeout').on('click', function() {
    const newTimeout = parseInt($('#queryTimeout').val());
    const button = $(this);
    const originalText = button.text();

    button.prop('disabled', true).text('Saving...');
    $.ajax({
        url: '/api/update-query-timeout',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({
            timeoutSecs: newTimeout * 60 // Convert to seconds
        }),
        success: function() {
            currentTimeout = newTimeout;
            button.hide();
            showToast('Query timeout updated successfully', 'success');
        },
        error: function(xhr) {
            showToast('Failed to update timeout', 'error');
        },
        complete: function() {
            button.prop('disabled', false).text(originalText);
        }
    });
});
