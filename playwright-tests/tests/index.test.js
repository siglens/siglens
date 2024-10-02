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

    test('verify date picker functionality', async ({ page }) => {
        await testDateTimePicker(page);
    });

    test('verify search and show records functionality', async ({ page }) => {
        await testDateTimePicker(page);

        const searchButton = page.locator('#query-builder-btn');

        await searchButton.click();
        await page.waitForTimeout(1000);
        await expect(page.locator('#logs-result-container')).toBeVisible();

        const showRecordsBtn = page.locator('#show-record-intro-btn');
        await expect(showRecordsBtn).toBeVisible();

        await showRecordsBtn.click();
        await expect(page.locator('div[aria-describedby="show-record-popup"]')).toBeVisible();

        const cancelRecordsBtn = page.locator('.cancel-record-btn');

        await cancelRecordsBtn.click();
        await expect(page.locator('div[aria-describedby="show-record-popup"]')).not.toBeVisible();
    });

    test('should switch between Builder and Code tabs', async ({ page }) => {
        await expect(page.locator('#tabs-1')).toBeVisible();
        await expect(page.locator('#tabs-2')).not.toBeVisible();

        await page.click('#tab-title2');
        await expect(page.locator('#tabs-1')).not.toBeVisible();
        await expect(page.locator('#tabs-2')).toBeVisible();

        await page.click('#tab-title1');
        await expect(page.locator('#tabs-1')).toBeVisible();
        await expect(page.locator('#tabs-2')).not.toBeVisible();
    });

    test('should open and close settings', async ({ page }) => {
        const settingsContainer = page.locator('#setting-container');
        const settingsButton = page.locator('#logs-settings');

        await expect(settingsContainer).not.toBeVisible();
        await settingsButton.click();
        await expect(settingsContainer).toBeVisible();
        await settingsButton.click();
        await expect(settingsContainer).not.toBeVisible();
    });

    test('should change query language', async ({ page }) => {
        await page.click('#logs-settings');
        await page.click('#query-language-btn');
        await page.click('#option-1');
        await expect(page.locator('#query-language-btn span')).toHaveText('SQL');
    });

    test('should change query mode', async ({ page }) => {
        await page.click('#logs-settings');
        await page.click('#query-mode-btn');
        await page.click('#mode-option-2');
        await expect(page.locator('#query-mode-btn span')).toHaveText('Code');
    });

    test('should change theme', async ({ page }) => {
        await testThemeToggle(page);
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

    test('should change table view', async ({ page }) => {
        await page.click('#log-opt-single-btn');
        await page.click('#log-opt-multi-btn');
        await page.click('#log-opt-table-btn');
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
