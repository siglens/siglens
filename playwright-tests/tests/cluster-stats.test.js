const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('Cluster Stats Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/cluster-stats.html');

    // Check for the presence of key elements
    await expect(page.locator('#app-side-nav')).toBeVisible();
    await expect(page.locator('#app-content-area')).toBeVisible();
    await expect(page.locator('#cstats-app-footer')).toBeVisible();

    // Check the time picker
    const datePickerBtn = page.locator('#date-picker-btn');
    await expect(datePickerBtn).toBeVisible();
    await datePickerBtn.click();
    await expect(page.locator('.daterangepicker')).toBeVisible();

    // Check for the presence of time range options
    const timeRanges = ['1 Hr', '3 Hrs', '6 Hrs', '12 Hrs', '24 Hrs', '7 Days', '30 Days', '90 Days', '180 Days', '1 Year'];
    for (const range of timeRanges) {
        await expect(page.locator(`.range-item:text("${range}")`)).toBeVisible();
    }

    // Check for the presence of stats sections
    await expect(page.locator('text=Logs Stats')).toBeVisible();
    await expect(page.locator('text=Metrics Stats')).toBeVisible();
    await expect(page.locator('text=Traces Stats')).toBeVisible();
    await expect(page.locator('text=Query Stats')).toBeVisible();

    // Check for the presence of charts
    await expect(page.locator('#EventCountChart-logs')).toBeVisible();
    await expect(page.locator('#GBCountChart-logs')).toBeVisible();
    await expect(page.locator('#EventCountChart-metrics')).toBeVisible();
    await expect(page.locator('#GBCountChart-metrics')).toBeVisible();
    await expect(page.locator('#EventCountChart-trace')).toBeVisible();
    await expect(page.locator('#GBCountChart-trace')).toBeVisible();

    // Check for the presence of data tables
    await expect(page.locator('#index-data-table')).toBeVisible();
    await expect(page.locator('#metrics-data-table')).toBeVisible();
    await expect(page.locator('#trace-data-table')).toBeVisible();
    await expect(page.locator('#query-table')).toBeVisible();

    // Check for the presence of ind-0 in the table
    await expect(page.locator('#index-data-table >> text=ind-0')).toBeVisible();

    // Delete index
    const deleteButton = page.locator('#index-del-btn-ind-0');
    await expect(deleteButton).toBeVisible();
    await deleteButton.click();
    await expect(page.locator('#confirm-del-index-prompt')).toBeVisible();
    await expect(page.locator('#del-index-btn')).toBeDisabled();
    await page.click('#cancel-del-index-btn');
    await expect(page.locator('#confirm-del-index-prompt')).toBeHidden();

    await expect(page.locator('#index-data-table >> text=ind-0')).toBeVisible();

    //Theme button
    await testThemeToggle(page);
});


