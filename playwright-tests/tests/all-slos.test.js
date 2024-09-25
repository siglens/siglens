const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('SLO Screen Test', () => {
    test('should have SLOs div with correct heading and message', async ({ page }) => {
        // Navigate to the page containing the SLOs div
        await page.goto('http://localhost:5122/all-slos.html');

        // Check if the SLOs div is present
        const slosDiv = page.locator('#all-slos');
        await expect(slosDiv).toBeVisible();

        // Check if the heading is correct
        await expect(slosDiv.locator('h1.myOrg-heading')).toHaveText('SLOs');

        // Check if the enterprise version message is present
        await expect(slosDiv).toContainText('This feature is available in Enterprise version');

        // Theme Button
        await testThemeToggle(page);
    });
});
