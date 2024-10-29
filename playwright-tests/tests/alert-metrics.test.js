const { test, expect } = require('@playwright/test');

test.describe('Alert Tests', () => {
    let alertName;

    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:5122/all-alerts.html');
    });

    test('Create a new metrics alert', async ({ page }) => {
        await test.step('Navigate to create alert page', async () => {
            await page.click('#new-alert-rule');
            await expect(page.locator('#alert-rule-name')).toBeVisible();
        });

        await test.step('Fill out alert form', async () => {
            alertName = `Metric Alert ${Date.now()}`;
            await page.fill('#alert-rule-name', alertName);

            await page.click('#alert-data-source');
            await page.click(`#data-source-options #option-2`);

            await page.click('#date-picker-btn');
            await page.click('#now-90d');
        });

        await test.step('Select metric', async () => {
            await page.click('#select-metric-input');
            await page.waitForSelector('.metrics-ui-widget .ui-menu-item');
            await page.click('.metrics-ui-widget .ui-menu-item:first-child');

            const inputValue = await page.inputValue('#select-metric-input');
            expect(inputValue).not.toBe('');
        });

        await test.step('Configure alert conditions', async () => {
            await page.click('#alert-condition');
            await page.click('.alert-condition-options #option-0');
            await page.fill('#threshold-value', '100');
            await page.fill('#evaluate-every', '5');
            await page.fill('#evaluate-for', '10');
        });

        await test.step('Add contact point', async () => {
            await page.click('#contact-points-dropdown');
            await page.click('.contact-points-options li:nth-child(1)');
            await expect(page.locator('#add-new-contact-popup')).toBeVisible();

            await page.fill('#contact-name', 'Test Contact');
            await page.click('#contact-types');
            await page.click('.contact-options #option-0');
            await page.fill('#slack-channel-id', 'test-channel-id');
            await page.fill('#slack-token', 'xoxb-your-slack-token');
            await page.click('#save-contact-btn');
        });

        await test.step('Finalize and save alert', async () => {
            await page.fill('#notification-msg', 'This is a test alert notification.');
            await page.click('.add-label-container');
            await page.fill('.label-container #label-key', 'TestLabel');
            await page.fill('.label-container #label-value', 'TestValue');
            await page.click('#save-alert-btn');
            await expect(page.locator('.ag-root-wrapper')).toBeVisible();
        });
    });

    test.afterEach(async ({ page }) => {
        if (alertName) {
            await deleteAlertIfExists(page, alertName);
        }
    });
});

async function deleteAlertIfExists(page, alertName) {
    await page.goto('http://localhost:5122/all-alerts.html');
    await page.waitForSelector('.ag-row', { state: 'attached' });

    const alertRow = page.locator(`.ag-row:has-text("${alertName}")`);
    if ((await alertRow.count()) > 0) {
        const rowCountBefore = await page.locator('.ag-row').count();

        await alertRow.locator('#delbutton').click();

        await expect(page.locator('.popupContent')).toBeVisible();
        await expect(page.locator('#delete-alert-name')).toContainText('Are you sure');
        await page.locator('#delete-btn').click();
        await expect(page.locator('.popupContent')).not.toBeVisible();

        await expect(async () => {
            const rowCountAfter = await page.locator('.ag-row').count();
            expect(rowCountAfter).toBeLessThan(rowCountBefore);
        }).toPass({ timeout: 5000 });
    } else {
        console.log(`Alert "${alertName}" not found in the grid. Skipping deletion.`);
    }
}
