const { test, expect } = require('@playwright/test');
const { createAlert } = require('./common-functions');

test.describe('All Alerts Screen Flow', () => {
    test('should perform the full flow on the All Alerts screen', async ({ page }) => {
        // Create a new logs alert
        await createAlert(page, 'Logs', 'option-1', 'option-1', 'city=Boston | stats count AS Count BY weekday');

        await page.waitForSelector('.ag-root-wrapper');

        const rowCount = await page.locator('.ag-row').count();
        expect(rowCount).toBeGreaterThan(0);

        // Test edit alert
        await page.locator('#editbutton').first().click();
        await expect(page).toHaveURL(/alert\.html\?id=[a-f0-9\-]+/);

        await page.fill('#alert-rule-name', `Update Test Alert ${Date.now()}`);

        await page.click('#save-alert-btn');

        await expect(page).toHaveURL(/all-alerts\.html/);

        // Test alert details
        const firstRow = page.locator('.ag-center-cols-container .ag-row[row-index="0"]').first();
        await firstRow.click();
        expect(page.url()).toContain('alert-details.html');

        await page.goBack();
        await expect(page).toHaveURL(/all-alerts\.html/);

        // Test mute alert
        await page.locator('#mute-icon').first().click();
        const dropdown = page.locator('.custom-alert-dropdown-menu').first();
        await dropdown.locator('#now-5m').click();
        await expect(page.locator('#mute-icon.muted').first()).toBeVisible();

        // Test delete alert
        const rowCountBefore = await page.locator('.ag-row').count();
        await page.locator('#delbutton').first().click();
        await expect(page.locator('.popupContent')).toBeVisible();
        await expect(page.locator('#delete-alert-name')).toContainText('Are you sure');
        await page.locator('#delete-btn').click();
        await expect(page.locator('.popupContent')).not.toBeVisible();

        const rowCountAfter = await page.locator('.ag-row').count();
        expect(rowCountAfter).toBeLessThan(rowCountBefore);
    });
});
