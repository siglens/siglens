const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('Cluster Stats Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/cluster-stats.html');

    // Check for the presence of key elements
    await expect(page.locator('#app-side-nav')).toBeVisible();
    await expect(page.locator('#app-content-area')).toBeVisible();
    await expect(page.locator('#cstats-app-footer')).toBeVisible();


    // Check for the presence of stats sections
    await expect(page.locator('text=Logs Stats')).toBeVisible();
    await expect(page.locator('text=Metrics Stats')).toBeVisible();
    await expect(page.locator('text=Traces Stats')).toBeVisible();

    // Check for the presence of data tables
    await expect(page.locator('#index-data-table')).toBeVisible();
    await expect(page.locator('#metrics-data-table')).toBeVisible();
    await expect(page.locator('#trace-data-table')).toBeVisible();

    //Theme button
    await testThemeToggle(page);
});
