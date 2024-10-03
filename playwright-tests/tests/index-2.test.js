const { test, expect } = require('@playwright/test');
const { testDateTimePicker, testThemeToggle } = require('./common-functions');

test.describe('Logs Page Tests', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:5122/index.html');

        // Perform search to show results table
        await testDateTimePicker(page);
        await page.locator('#query-builder-btn').click();
        await page.waitForTimeout(1000);
        await expect(page.locator('#logs-result-container')).toBeVisible();
    });

    test('should add to alert', async ({ page, context }) => {
        await page.click('#alert-from-logs-btn');
        await expect(page.locator('.addrulepopupContent')).toBeVisible();

        await page.fill('#rule-name', 'Test Alert');

        // Wait for the new page to be created
        const newPagePromise = context.waitForEvent('page');

        await page.click('#addrule-save-btn');

        const newPage = await newPagePromise;
        await newPage.waitForLoadState();

        // Verify that the new page's URL contains 'alert.html'
        expect(newPage.url()).toContain('alert.html');

        await newPage.close();
    });

    test('should add to dashboard', async ({ page, context }) => {
        await page.click('#add-logs-to-db-btn');
        await expect(page.locator('#create-db-popup')).toBeVisible();

        await page.fill('#db-name', `Test Dashboard + ${Date.now()}`);

        // Wait for the new page to be created
        const newPagePromise = context.waitForEvent('page');

        await page.click('#create-db');

        const newPage = await newPagePromise;
        await newPage.waitForLoadState();

        // Verify that the new page's URL contains 'dashboard.html'
        expect(newPage.url()).toContain('dashboard.html');

        await newPage.close();
    });

    test('should add to existing dashboard', async ({ page, context }) => {
        await page.click('#add-logs-to-db-btn');
        await expect(page.locator('#create-db-popup')).toBeVisible();

        await page.click('.existing-dashboard-btn');

        await page.click('#selected-dashboard');
        await page.click('#dashboard-options li:first-child'); // Selects the first dashboard in the list

        const newPagePromise = context.waitForEvent('page');

        await page.click('#create-panel');

        const newPage = await newPagePromise;
        await newPage.waitForLoadState();

        // Verify that the new page's URL contains 'dashboard.html'
        expect(newPage.url()).toContain('dashboard.html');
        await newPage.close();
    });

    test('should save query', async ({ page }) => {
        await page.click('#saveq-btn');

        // Wait for the dialog to be visible
        const dialog = page.locator('[aria-describedby="save-queries"]');
        await dialog.waitFor({ state: 'visible' });

        // Fill in the form
        await page.fill('[aria-describedby="save-queries"] #qname', 'Test-Query');
        await page.fill('[aria-describedby="save-queries"] #description', 'This is a test query');

        // Click the Save button within the dialog
        const saveButton = dialog.locator('.saveqButton');
        await saveButton.waitFor({ state: 'visible' });
        await saveButton.click();

        const toast = page.locator('#message-toast');
        const toastText = await toast.innerText();
        const normalizedToastText = toastText.replace(/\s+/g, ' ').trim();
        expect(normalizedToastText).toContain('Query saved successfully');
    });

    test('should download logs', async ({ page }) => {
        await page.click('.download-all-logs-btn');
        await page.click('#csv-block');
        const dialog = page.locator('[aria-describedby="download-info"]');
        await dialog.waitFor({ state: 'visible' });

        // Fill in the form
        await page.fill('#qnameDL', 'Test-CSV-Download');

        // Click the Save button within the dialog
        const saveButton = dialog.locator('.saveqButton');
        await saveButton.click();

        // Verify download started
        const downloadPromise = page.waitForEvent('download');
        await downloadPromise;
    });

    test('should toggle available fields', async ({ page }) => {
        // Open the available fields dropdown
        await page.click('#avail-fields-btn');
        await expect(page.locator('#available-fields')).toBeVisible();

        // Click on the 'app_name' field
        await page.click('#available-fields .fields .available-fields-dropdown-item[data-index="app_name"]');

        await page.waitForTimeout(1000);

        // Verify the field was toggled (column should not be visible in the grid)
        await expect(page.locator('.LogResultsGrid')).not.toContainText('app_name');
    });
});
