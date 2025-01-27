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
    {{ .Button1Function }}
});

$('#diagnostics-btn').on('click', function () {
    const originalHtml = '<span class="white-download-icon"></span> Download Diagnostic Data';
    $(this).attr('disabled', true).html('<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Downloading...');

    $.ajax({
        url: '/api/collect-diagnostics',
        method: 'GET',
        xhrFields: {
            responseType: 'blob',
        },
        success: function (blob) {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `siglens-diagnostics-${new Date().toISOString()}.zip`;
            document.body.appendChild(a);
            a.click();

            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        },
        error: function () {
            showToast('Failed to fetch diagnostic data', 'error');
        },
        complete: function () {
            $('#diagnostics-btn').attr('disabled', false).html(originalHtml);
        },
    });
});
