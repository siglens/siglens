const { test, expect } = require('@playwright/test');

test.describe('Metrics Alert Tests', () => {
    let alertName;

    test.setTimeout(90000);

    test('Create a new metrics alert', async ({ page }) => {
        await page.goto('http://localhost:5122/all-alerts.html', {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await test.step('Navigate to create alert page', async () => {
            await page.click('#new-alert-rule');
        });

        await test.step('Select alert type card', async () => {
            await page.click('#create-metrics-alert');
            await expect(page.locator('#alert-rule-name')).toBeVisible({ timeout: 30000 });
        });

        await test.step('Fill out alert form', async () => {
            alertName = `Metric Alert ${Date.now()}`;
            await page.fill('#alert-rule-name', alertName);

            await page.click('#date-picker-btn');
            await page.click('#now-90d');
        });

        await test.step('Select metric', async () => {
            await page.click('#select-metric-input');
            await page.waitForSelector('.metrics-ui-widget .ui-menu-item', { timeout: 20000 });
            await page.waitForTimeout(1000);
            await page.click('.metrics-ui-widget .ui-menu-item:first-child');

            await page.waitForTimeout(1500);
            const inputValue = await page.inputValue('#select-metric-input');
            expect(inputValue).not.toBe('');
        });

        await test.step('Configure alert conditions', async () => {
            await expect(page.locator('#alert-condition')).toBeVisible({ timeout: 10000 });
            await expect(page.locator('#alert-condition')).toBeEnabled({ timeout: 10000 });
            await page.waitForTimeout(1000);

            await page.click('#alert-condition');

            await expect(page.locator('.alert-condition-options')).toBeVisible({ timeout: 15000 });
            await page.waitForTimeout(1000);

            await page.locator('.alert-condition-options #option-0').click({ force: true, timeout: 10000 });

            await page.waitForTimeout(1000);

            await page.fill('#threshold-value', '100');

            await expect(page.locator('#evaluate-every')).toBeVisible({ timeout: 10000 });
            await expect(page.locator('#evaluate-every')).toBeEnabled({ timeout: 10000 });
            await page.waitForTimeout(500);

            await page.fill('#evaluate-every', '5');

            await expect(page.locator('#evaluate-for')).toBeVisible({ timeout: 10000 });
            await page.waitForTimeout(500);

            await page.fill('#evaluate-for', '10');
        });

        await test.step('Add contact point', async () => {
            await page.click('#contact-points-dropdown');
            await expect(page.locator('.contact-points-options')).toBeVisible({ timeout: 10000 });
            await page.waitForTimeout(500);
            await page.click('.contact-points-options li:nth-child(1)');
            await expect(page.locator('#add-new-contact-popup')).toBeVisible({ timeout: 15000 });

            await page.fill('#contact-name', 'Test Contact');
            await page.click('#contact-types');
            await page.waitForTimeout(500);
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
            await expect(page.locator('.ag-root-wrapper')).toBeVisible({ timeout: 30000 });
        });

        await deleteAlertIfExists(page, alertName);
    });
});

async function deleteAlertIfExists(page, alertName) {
    try {
        await page.goto('http://localhost:5122/all-alerts.html', {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await page.waitForSelector('.ag-row', {
            state: 'attached',
            timeout: 30000,
        });

        await page.waitForTimeout(2000);

        const alertRow = page.locator(`.ag-row:has-text("${alertName}")`);
        const count = await alertRow.count();

        if (count > 0) {
            await alertRow.locator('#delbutton').click();
            await expect(page.locator('.popupContent')).toBeVisible({ timeout: 10000 });
            await page.locator('#delete-btn').click();
            await expect(page.locator('.popupContent')).not.toBeVisible({ timeout: 10000 });
        }
    } catch (error) {
        console.error(`Error during alert deletion: ${error.message}`);
    }
}
