const { test, expect } = require('@playwright/test');
test.describe('All Alerts Screen Flow', () => {
    let page;
    let alertName;
    test.beforeAll(async ({ browser }) => {
        page = await browser.newPage();
        await page.goto('http://localhost:5122/all-alerts.html');
    });

    test.afterAll(async () => {
        await page.close();
    });

    test('should create a new logs alert', async () => {
        await test.step('Navigate to create alert page', async () => {
            await page.click('#new-alert-rule');
            await expect(page.locator('#alert-rule-name')).toBeVisible();
        });

        await test.step('Fill out alert form', async () => {
            alertName = `Test Alert ${Date.now()}`;
            await page.fill('#alert-rule-name', alertName);

            await page.click('#alert-data-source');
            await page.click(`#data-source-options #option-1`);

            await page.click('#date-picker-btn');
            await page.click('#now-90d');

            await page.click('#logs-language-btn');
            await page.click(`#logs-language-options #option-1`);
        });

        await test.step('Configure alert conditions', async () => {
            await page.click('#tab-title2');
            await page.fill('#filter-input', 'city=Boston | stats count AS Count BY weekday');
            await page.click('#run-filter-btn');

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

    test('should edit the created alert', async () => {
        test.setTimeout(60000);
        await test.step('Navigate to edit page', async () => {
            await page.click('#editbutton:first-child');
            await expect(page).toHaveURL(/alert\.html\?id=[a-f0-9\-]+/);
        });

        await test.step('Update alert name', async () => {
            const updatedName = `Updated Test Alert ${Date.now()}`;
            await page.fill('#alert-rule-name', updatedName);
            
            await Promise.all([
                page.waitForNavigation({ url: '**/all-alerts.html' }),
                page.click('#save-alert-btn')
            ]);

            await expect(page).toHaveURL(/all-alerts\.html/);
        });
    });

    test('should view alert details', async () => {
        await page.click('.ag-center-cols-container .ag-row[row-index="0"]');
        await expect(page).toHaveURL(/alert-details\.html/);
        await page.goBack();
    });

    test('should mute the alert', async () => {
        await page.click('#mute-icon:first-child');
        await page.click('.custom-alert-dropdown-menu #now-5m');
        await expect(page.locator('#mute-icon.muted:first-child')).toBeVisible();
    });

    test('should delete the created alert', async () => {
        const alertRow = page.locator(`.ag-row:has-text("${alertName}")`);
        if ((await alertRow.count()) > 0) {
            const rowCountBefore = await page.locator('.ag-row').count();

            await alertRow.locator('#delbutton').click();
            await expect(page.locator('.popupContent')).toBeVisible();
            await expect(page.locator('#delete-alert-name')).toContainText('Are you sure');
            await page.click('#delete-btn');
            await expect(page.locator('.popupContent')).not.toBeVisible();

            await expect(async () => {
                const rowCountAfter = await page.locator('.ag-row').count();
                expect(rowCountAfter).toBeLessThan(rowCountBefore);
            }).toPass();
        }
    });
});
