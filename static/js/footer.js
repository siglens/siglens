// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(function () {
    const footerComponent = `
        <span id="year"></span> &copy; SigLens
    `;

    $('#app-footer').prepend(footerComponent);
    $('#cstats-app-footer').prepend(footerComponent);
    $('#year').text(new Date().getFullYear());
});