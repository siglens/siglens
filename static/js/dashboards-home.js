// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);
    const { loadData } = initializeDashboardPage();
    await loadData();
});
