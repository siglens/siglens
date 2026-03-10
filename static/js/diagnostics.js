// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(function () {
    $('.theme-btn').on('click', themePickerHandler);
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
