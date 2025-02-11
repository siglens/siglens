const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('Application Version Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/application-version.html');

    await page.waitForSelector('#versionInfo:not(:empty)', { timeout: 10000 });
    const versionInfoText = await page.locator('#versionInfo').innerText();
    // Check if the version info contains the expected text
    expect(versionInfoText).toContain('SigLens Version:');

    // Theme Button
    await testThemeToggle(page);
});
