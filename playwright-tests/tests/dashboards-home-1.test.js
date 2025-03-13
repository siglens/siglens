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

    test('should search and filter dashboards', async ({ page }) => {
        // Wait for grid to be ready
        await page.waitForSelector('.ag-center-cols-container');
        await page.waitForLoadState('networkidle');

        // Search for "Dashboard"
        await page.fill('#search-input', 'Dashboard B');
        await page.waitForTimeout(1000);

        // Verify dashboard items
        const dashboardRows = await page.locator('.ag-center-cols-container .ag-row').count();
        expect(dashboardRows).toBe(1); // Should show Dashboard B

        // Clear search
        await page.fill('#search-input', '');
        await page.waitForTimeout(1000);

        // Search for "Folder"
        await page.fill('#search-input', 'Folder');
        await page.waitForTimeout(1000);

        // Verify folder items
        const folderRows = await page.locator('.ag-center-cols-container .ag-row').count();
        expect(folderRows).toBe(1); // Should show Folder A
    });

});
