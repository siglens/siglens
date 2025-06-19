const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('License Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/license.html');

    // Check if License Info section is present (only for non-SaaS mode)
    const licenseInfoHeader = page.locator('text=License Information');
    if (await licenseInfoHeader.count() > 0) {
        await expect(licenseInfoHeader).toBeVisible();

        // Check License Info Table rows
        const tableChecks = [
            { header: 'License Type', id: 'licenseType' },
            { header: 'Issued To', id: 'licensedTo' },
            { header: 'Organization', id: 'organization' },
            { header: 'Version', id: 'version' },
            { header: 'Max Users', id: 'maxUsers' },
            { header: 'Expires On', id: 'licenseExpiry' }
        ];

        for (const check of tableChecks) {
            // Check if header exists
            await expect(page.locator('th', { hasText: check.header }))
                .toBeVisible();

            // Check if value is loaded (not showing loading placeholder)
            await expect(page.locator(`td#${check.id}`))
                .not.toHaveText('Loading...');

            // Verify content is present
            await expect(page.locator(`td#${check.id}`))
                .not.toBeEmpty();
        }
    }

    // UI container checks
    await expect(page.locator('.myOrg'))
        .toBeVisible();
    await expect(page.locator('.myOrg-container'))
        .toBeVisible();

    // Check footer presence
    await expect(page.locator('#app-footer'))
        .toBeVisible();

    // Test theme toggle functionality
    await testThemeToggle(page);
});