const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('Settings Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/org-settings.html');

    // Organization settings are displayed correctly
    await expect(page.locator('#orgName')).toHaveText('SigLens');
    await expect(page.locator('#deployment-type')).toHaveText('Single Node Deployment');

    // System information table is populated
    const systemInfoTable = page.locator('#system-info-table');
    await expect(systemInfoTable).toBeVisible();

    // Check if key system info rows are present
    const expectedRows = ['Operating System', 'vCPUs', 'Memory Usage', 'Disk Usage', 'Process Uptime'];
    for (const row of expectedRows) {
        await expect(systemInfoTable.locator(`text=${row}`)).toBeVisible();
    }

    // Theme Button
    await testThemeToggle(page);
});
