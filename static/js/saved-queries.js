// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(() => {
    $('#app-container').on('click', '.theme-btn', themePickerHandler);
    setupEventHandlers();
    getSavedQueries();
});
