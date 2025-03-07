const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('Minion Searches Page Tests', () => {
    test('should load minion searches page correctly', async ({ page }) => {
        // Navigate to the minion searches page
        await page.goto('http://localhost:5122/minion-searches.html');

        // Check if all log level buttons are present
        const logLevels = ['Error', 'Warning', 'Debug', 'Info'];
        for (const level of logLevels) {
            const button = page.locator(`.log-pane button#${level.toLowerCase()}`);
            await expect(button).toBeVisible();
            await expect(button.locator('h3')).toHaveText(level);
            await expect(button.locator('h4')).toHaveText('0');
        }

        // Check if the ag-grid is present
        await expect(page.locator('#ag-grid')).toBeVisible();
        await expect(page.locator('.ag-root-wrapper')).toBeVisible();

        // Check if the grid headers are correct
        const expectedHeaders = ['Log Statement', 'Filename', 'Line Number', 'Level', 'State'];
        const headers = page.locator('.ag-header-cell-text');
        await expect(headers).toHaveCount(expectedHeaders.length);
        for (let i = 0; i < expectedHeaders.length; i++) {
            await expect(headers.nth(i)).toHaveText(expectedHeaders[i]);
        }

        // Check if the "No Rows To Show" message is visible
        await expect(page.locator('.ag-overlay-no-rows-center')).toHaveText('No Rows To Show');

        // Check if the pagination panel is present
        await expect(page.locator('.ag-paging-panel')).toBeVisible();

        // Check if the result pane is present
        await expect(page.locator('.result-pane .header')).toHaveText('Result');
        await expect(page.locator('.result-pane .result-body')).toBeVisible();

        // Theme Button
        await testThemeToggle(page);
    });
});
