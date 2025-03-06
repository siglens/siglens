const { test, expect } = require('@playwright/test');
const { testDateTimePicker, testThemeToggle } = require('./common-functions');

test.describe('Metrics Explorer Tests', () => {

    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:5122/metrics-explorer.html');
    });

    test('verify date picker functionality', async ({ page }) => {
        await testDateTimePicker(page);
    });


    test('Verify page loads correctly', async ({ page }) => {
        await expect(page.locator('#metrics-graphs')).toBeVisible();
        await expect(page.locator('.graph-view-container')).toBeVisible();
        await expect(page.locator('#visualization-options')).toBeVisible();
        await expect(page.locator('#metrics-explorer')).toBeVisible();
    });

    test('Create a new metrics query', async ({ page }) => {
        await test.step('Select metric', async () => {
            await page.click('#select-metric-input');
            await page.waitForSelector('.metrics-ui-widget .ui-menu-item');
            await page.click('.metrics-ui-widget .ui-menu-item:first-child');

            const mname = await page.inputValue('#select-metric-input');
            expect(mname).not.toBe('');
        });
    });

    test('should add to alert', async ({ page, context }) => {
       
        // Wait for the new page to be created
        const newPagePromise = context.waitForEvent('page');
        await page.click('#alert-from-metrics-btn');

        const newPage = await newPagePromise;
        await newPage.waitForLoadState();

        // Verify that the new page's URL contains 'alert.html'
        expect(newPage.url()).toContain('alert.html');

        await newPage.close();
    });


    test('should add to dashboard', async ({ page, context }) => {
        await page.click('#add-metrics-to-db-btn');
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
        await page.click('#add-metrics-to-db-btn');
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

    test('should download metrics', async ({ page }) => {
        await page.click('.download-all-logs-btn');
        await page.click('#csv-block');
    });

    test('should toggle one graph per query button', async ({ page }) => {
        await expect(page.locator('.graph-view-container')).toBeVisible();
        await expect(page.locator('#metrics-graphs')).toBeVisible();
        await expect(page.locator('#merged-graph-container')).not.toBeVisible();

        await page.click('.switch');
        await expect(page.locator('#metrics-graphs')).not.toBeVisible();
        await expect(page.locator('#merged-graph-container')).toBeVisible();

    });
    test('should change theme', async ({ page }) => {
        await testThemeToggle(page);
    });
    
    test('should show formula options', async ({ page }) => {
        await page.click('.show-functions');
        await expect(page.locator('.options-container')).toBeVisible();
        await expect(page.locator('#functions-search-box')).toBeVisible();        
        
    });

    test('should switch between Builder and raw query tabs', async ({ page }) => {
        await expect(page.locator('.raw-query-btn')).toBeVisible();
        await expect(page.locator('.query-builder')).toBeVisible();
        await expect(page.locator('.raw-query')).not.toBeVisible();

        await page.click('.raw-query-btn');
        await expect(page.locator('.query-builder')).not.toBeVisible();
        await expect(page.locator('.raw-query')).toBeVisible();
    });


    test('test add query', async ({ page }) => {
        await page.click('#add-query');
        const firstMetricsQuery = page.locator('.metrics-query').nth(0);
        await expect(firstMetricsQuery).toBeVisible();
        const firstMetricsQueryName = page.locator('.query-name').nth(0);
        await expect(firstMetricsQueryName).toHaveText('a');
        const secMetricsQuery = page.locator('.metrics-query').nth(1);
        await expect(secMetricsQuery).toBeVisible();
        const secMetricsQueryName = page.locator('.query-name').nth(1);
        await expect(secMetricsQueryName).toHaveText('b');
    });
    

    test('test add formula', async ({ page }) => {
        await page.click('#add-formula');
        await expect(page.locator('#metrics-formula')).toBeVisible();
    });

    test('should show display options', async ({ page }) => {
        await page.click('#display-input');
        await page.click('#ui-id-1');
        await expect(page.locator('#display-input')).toHaveValue('Bar chart');

    });

    test('should show color options', async ({ page }) => {
        await page.click('#color-input');
        await page.click('#ui-id-2');
        await expect(page.locator('#color-input')).toHaveValue('Green');

    });

    test('should show style options', async ({ page }) => {
        await page.click('#line-style-input');
        await page.click('#ui-id-3');
        await expect(page.locator('#line-style-input')).toHaveValue('Dash');

    });

    test('should show stroke options', async ({ page }) => {
        await page.click('#stroke-input');
        await page.click('#ui-id-4');
        await expect(page.locator('#stroke-input')).toHaveValue('Thin');

    });

});