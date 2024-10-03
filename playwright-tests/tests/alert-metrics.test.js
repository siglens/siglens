const { test, expect } = require('@playwright/test');

test.describe('Alert Tests', () => {
    let alertName;
    test.beforeEach(async ({ page }) => {
        console.log('Navigating to the All Alerts page...');
        await page.goto('http://localhost:5122/all-alerts.html');
    });

    test('Create a new metrics alert', async ({ page }) => {
        test.setTimeout(120000);
        console.log('Starting test to create a new alert...');
        await page.click('#new-alert-rule');

        // Wait for the alert page to load
        console.log('Waiting for alert rule name field to be visible...');
        await page.waitForSelector('#alert-rule-name', { state: 'visible' });

        // Fill out the alert form
        alertName = `Metric Alert ${Date.now()}`;
        console.log(`Alert Name generated: ${alertName}`);
        await page.fill('#alert-rule-name', alertName);

        // Select data source (Logs or Metrics)
        console.log('Selecting the data source...');
        await page.click('#alert-data-source');
        await page.click(`#data-source-options #option-2`);

        console.log('Setting date range...');
        await page.click('#date-picker-btn');
        await page.click('#now-90d');

        console.log('Selecting metric...');
        await page.click('#select-metric-input');
        await page.waitForSelector('.metrics-ui-widget .ui-menu-item');
        await page.click('.metrics-ui-widget .ui-menu-item:first-child');

        const inputValue = await page.inputValue('#select-metric-input');
        console.log(`Metric selected: ${inputValue}`);

        expect(inputValue).not.toBe('');

        console.log('Setting alert condition...');
        await page.click('#alert-condition');
        await page.click('.alert-condition-options #option-0'); // Select "Is above"
        await page.fill('#threshold-value', '100');

        // Set evaluation interval
        console.log('Setting evaluation intervals...');
        await page.fill('#evaluate-every', '5');
        await page.fill('#evaluate-for', '10');

        // Open contact point dropdown
        console.log('Opening contact point dropdown...');
        await page.click('#contact-points-dropdown');

        // Add new contact point (Slack)
        console.log('Adding a new contact point...');
        await page.click('.contact-points-options li:nth-child(1)'); // Select the "Add New" option

        // Wait for the contact form popup to appear
        console.log('Waiting for new contact point popup...');
        await page.waitForSelector('#add-new-contact-popup', { state: 'visible' });

        // Fill out the contact form (Slack)
        console.log('Filling out the contact form...');
        await page.fill('#contact-name', 'Test Contact');
        await page.click('#contact-types'); // Open the type dropdown
        await page.click('.contact-options #option-0'); // Select "Slack"

        // Fill out Slack details
        console.log('Filling Slack channel and token...');
        await page.fill('#slack-channel-id', 'test-channel-id');
        await page.fill('#slack-token', 'xoxb-your-slack-token');

        // Save the contact point
        console.log('Saving the contact point...');
        await page.click('#save-contact-btn');

        // Fill notification message
        console.log('Filling the notification message...');
        await page.fill('#notification-msg', 'This is a test alert notification.');

        // Add a custom label
        console.log('Adding a custom label...');
        await page.click('.add-label-container');
        await page.fill('.label-container #label-key', 'TestLabel');
        await page.fill('.label-container #label-value', 'TestValue');

        // Save the alert
        console.log('Saving the alert...');
        await page.click('#save-alert-btn');
        await page.waitForNavigation({ url: /all-alerts\.html$/, timeout: 60000 });
        expect(page.url()).toContain('all-alerts.html');

        // Verify the alert was created
        console.log(`Verifying if alert "${alertName}" is visible...`);
        await expect(page.locator(`text=${alertName}`)).toBeVisible();
    });

    test.afterEach(async ({ page }) => {
        if (alertName) {
            console.log(`Attempting to delete alert: ${alertName}`);
            await page.goto('http://localhost:5122/all-alerts.html');

            // Wait for the ag-grid to load
            await page.waitForSelector('.ag-row', { state: 'attached' });

            // Check if the alert is present in the grid
            const alertRow = page.locator(`.ag-row:has-text("${alertName}")`);
            if ((await alertRow.count()) > 0) {
                const rowCountBefore = await page.locator('.ag-row').count();
                console.log(`Row count before deletion: ${rowCountBefore}`);

                // Click the delete button for this alert
                console.log('Clicking delete button...');
                await alertRow.locator('#delbutton').click();

                // Handle the confirmation popup
                await expect(page.locator('.popupContent')).toBeVisible();
                await expect(page.locator('#delete-alert-name')).toContainText('Are you sure');
                await page.locator('#delete-btn').click();
                await expect(page.locator('.popupContent')).not.toBeVisible();

                // Verify that the row count has decreased
                await page.waitForTimeout(1000); // Short wait for the grid to update
                const rowCountAfter = await page.locator('.ag-row').count();
                console.log(`Row count after deletion: ${rowCountAfter}`);
                expect(rowCountAfter).toBeLessThan(rowCountBefore);
            } else {
                console.log(`Alert "${alertName}" not found in the grid. Skipping deletion.`);
            }
        }
    });
});
