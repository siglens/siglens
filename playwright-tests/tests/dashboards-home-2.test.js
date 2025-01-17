import { test, expect } from '@playwright/test';

test.describe('Dashboard Search and Sort Tests', () => {
    test.setTimeout(60000);

    test.beforeEach(async ({ page }) => {
        // Navigate to home page
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.waitForLoadState('networkidle');

        // Create Folder A
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-folder-btn');
        await page.fill('#folder-name', 'Folder A');

        // Click save and wait for both response and navigation
        await Promise.all([page.waitForResponse((response) => response.url().includes('/api/dashboards/folders/create')), page.click('#save-folder-btn'), page.waitForNavigation()]);

        // Navigate back to home
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.waitForLoadState('networkidle');

        // Create Dashboard B
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-db-btn');
        await page.fill('#db-name', 'Dashboard B');

        // Click save and wait for both response and navigation
        await Promise.all([page.waitForResponse((response) => response.url().includes('/api/dashboards/create')), page.click('#save-dbbtn'), page.waitForNavigation()]);

        // Final navigation back to home
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.waitForLoadState('networkidle');
    });

    test.afterEach(async ({ page }) => {
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.waitForLoadState('networkidle');

        // Delete items one by one
        while (true) {
            const deleteButton = await page.locator('#delbutton').first();
            if (!(await deleteButton.count())) break;

            await deleteButton.click();
            await page.fill('.confirm-input', 'Delete');

            await Promise.all([page.waitForResponse((response) => response.url().includes('/api/dashboards/')), page.click('.delete-btn')]);

            await page.waitForLoadState('networkidle');
        }
    });

    test('should filter starred items and sort by different criteria', async ({ page }) => {
        // Wait for grid to be ready
        await page.waitForSelector('.ag-center-cols-container');
        await page.waitForLoadState('networkidle');

        // Check initial row count
        const initialRows = await page.locator('.ag-center-cols-container .ag-row').count();
        expect(initialRows).toBe(3); // One folder and one dashboard and one default

        // Click starred filter
        await page.click('#starred-filter');
        await page.waitForTimeout(500);

        // Verify no rows are shown when starred filter is active
        const starredRows = await page.locator('.ag-center-cols-container .ag-row').count();
        expect(starredRows).toBe(0);

        // Uncheck starred filter
        await page.click('#starred-filter');
        await page.waitForTimeout(500);

        // Sort alphabetically A-Z
        await page.click('.sort-text');
        await page.click('li[data-sort="alpha-asc"]');
        await page.waitForTimeout(500);

        let sortedNames = await page.$$eval('.ag-center-cols-container .ag-row', (rows) => rows.map((row) => row.textContent?.trim()));

        // Sort alphabetically Z-A
        await page.click('.sort-text');
        await page.click('li[data-sort="alpha-desc"]');
        await page.waitForTimeout(500);

        sortedNames = await page.$$eval('.ag-center-cols-container .ag-row', (rows) => rows.map((row) => row.textContent?.trim()));

        // Clear sort
        await page.locator('.clear-sort').click();
        await page.waitForTimeout(500);

        // Verify original order is restored
        const finalRows = await page.locator('.ag-center-cols-container .ag-row').count();
        expect(finalRows).toBe(3);
    });
});

