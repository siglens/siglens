const { test, expect } = require('@playwright/test');

test.describe('All Alerts Screen Flow', () => {
    let alertName;
    test('should perform the full flow on the All Alerts screen', async ({ page }) => {
        await page.goto('http://localhost:5122/all-alerts.html');

        // Create a new logs alert
        await page.click('#new-alert-rule');

        // Wait for the alert page to load
        await page.waitForSelector('#alert-rule-name', { state: 'visible' });

        // Fill out the alert form
        alertName = `Test-Alert ${Date.now()}`;
        await page.fill('#alert-rule-name', alertName);

        // Select data source (Logs or Metrics)
        await page.click('#alert-data-source');
        await page.click(`#data-source-options #option-1`);

        await page.click('#date-picker-btn');
        await page.click('#now-90d');

        await page.click('#logs-language-btn');
        await page.click(`#logs-language-options #option-1`);

        await page.click('#tab-title2'); // Switch to Code tab
        await page.fill('#filter-input', 'city=Boston | stats count AS Count BY weekday');
        await page.click('#run-filter-btn'); // Run search

        await page.click('#alert-condition');
        await page.click('.alert-condition-options #option-0'); // Select "Is above"
        await page.fill('#threshold-value', '100');

        // Set evaluation interval
        await page.fill('#evaluate-every', '5');
        await page.fill('#evaluate-for', '10');

        // Open contact point dropdown
        await page.click('#contact-points-dropdown');

        // Add new contact point (Slack)
        await page.click('.contact-points-options li:nth-child(1)'); // Select the "Add New" option

        // Wait for the contact form popup to appear
        await page.waitForSelector('#add-new-contact-popup', { state: 'visible' });

        // Fill out the contact form (Slack)
        await page.fill('#contact-name', 'Test Contact');
        await page.click('#contact-types'); // Open the type dropdown
        await page.click('.contact-options #option-0'); // Select "Slack"

        // Fill out Slack details
        await page.fill('#slack-channel-id', 'test-channel-id');
        await page.fill('#slack-token', 'xoxb-your-slack-token');

        // Save the contact point
        await page.click('#save-contact-btn');

        // Fill notification message
        await page.fill('#notification-msg', 'This is a test alert notification.');

        // Add a custom label
        await page.click('.add-label-container');
        await page.fill('.label-container #label-key', 'TestLabel');
        await page.fill('.label-container #label-value', 'TestValue');

        // Save the alert
        await page.click('#save-alert-btn');
        
        await page.waitForSelector('.ag-root-wrapper');

        const rowCount = await page.locator('.ag-row').count();
        expect(rowCount).toBeGreaterThan(0);

        // Test edit alert
        await page.locator('#editbutton').first().click();
        await expect(page).toHaveURL(/alert\.html\?id=[a-f0-9\-]+/);

        alertName = `Update Test Alert ${Date.now()}`
        await page.fill('#alert-rule-name', alertName);

        await page.click('#save-alert-btn');
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
        await page.waitForSelector('.ag-row', { state: 'attached' });

        // Check if the alert is present in the grid
        const alertRow = page.locator(`.ag-row:has-text("${alertName}")`);
        if ((await alertRow.count()) > 0) {
            const rowCountBefore = await page.locator('.ag-row').count();

            // Click the delete button for this alert
            await alertRow.locator('#delbutton').click();

            // Handle the confirmation popup
            await expect(page.locator('.popupContent')).toBeVisible();
            await expect(page.locator('#delete-alert-name')).toContainText('Are you sure');
            await page.locator('#delete-btn').click();
            await expect(page.locator('.popupContent')).not.toBeVisible();

            // Verify that the row count has decreased
            await page.waitForTimeout(1000); // Short wait for the grid to update
            const rowCountAfter = await page.locator('.ag-row').count();
            expect(rowCountAfter).toBeLessThan(rowCountBefore);
        }
    });
});
