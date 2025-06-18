const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('License Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/license.html');

    // Check for License Information heading
    await expect(page.locator('text=License Information')).toBeVisible();

    // Check for summary/license card
    const card = page.locator('.license-card-container .card');
    await expect(card).toBeVisible();

    // Check for summary text
    await expect(page.locator('text=SigLens Observability Solution License')).toBeVisible();
    await expect(page.locator('text=GNU Affero General Public License v3.0')).toBeVisible();
    await expect(page.locator('text=Disclaimer')).toBeVisible();

    // Check for full license text heading and some license content
    await expect(page.locator('text=Full License Text')).toBeVisible();
    await expect(page.locator('pre')).toContainText('GNU AFFERO GENERAL PUBLIC LICENSE');
    await expect(page.locator('pre')).toContainText('TERMS AND CONDITIONS');

    // Check containers
    await expect(page.locator('.myOrg')).toBeVisible();
    await expect(page.locator('.myOrg-container')).toBeVisible();

    // Footer
    await expect(page.locator('#app-footer')).toBeVisible();

    // Theme Button
    await testThemeToggle(page);
});
