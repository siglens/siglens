const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('SLO Screen Test', () => {
    test('should have SLOs div with correct heading and message', async ({ page }) => {
        // Navigate to the page containing the SLOs div
        await page.goto('http://localhost:5122/all-slos.html');

        // Check if the enterprise version message is present
        await expect(slosDiv).toContainText('This feature is available in Enterprise version');

        // Theme Button
        await testThemeToggle(page);
    });
});
