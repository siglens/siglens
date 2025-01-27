import { test, expect } from '@playwright/test';

test.describe('Create Dashboard Tests', () => {
    let createdDashboardIds = [];

    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:5122/dashboards-home.html');
    });

    test.afterEach(async ({ page }) => {
        // Clean up dashboards using API calls
        for (const id of createdDashboardIds) {
            try {
                const response = await page.request.get(`http://localhost:5122/api/dashboards/delete/${id}`, {
                    headers: {
                        'Content-Type': 'application/json',
                        Accept: '*/*',
                    },
                });

                if (!response.ok()) {
                    console.error(`Failed to delete dashboard ${id}: ${response.statusText()}`);
                }
            } catch (error) {
                console.error(`Error deleting dashboard ${id}:`, error);
            }
        }
        createdDashboardIds = [];
    });

    test('should create new dashboard', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await expect(page.locator('#add-new-container .dropdown .dropdown-menu ')).toBeVisible();

        await page.click('#create-db-btn');
        await expect(page.locator('#new-dashboard-modal')).toBeVisible();

        await page.fill('#db-name', 'Test Dashboard');
        await page.fill('#db-description', 'Test Description');

        const navigationPromise = page.waitForResponse((response) => response.url().includes('/api/dashboards/create') && response.status() === 200);

        await page.click('#save-dbbtn');
        await navigationPromise;

        await expect(page).toHaveURL(/.*dashboard\.html\?id=/, { timeout: 10000 });
        
        const url = page.url();
        createdDashboardIds.push(url.split('id=')[1]);
    });

    test('should validate empty dashboard name', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-db-btn');

        await page.click('#save-dbbtn');

        // Try to save without entering name
        const errorTip = page.locator('#new-dashboard-modal .error-tip');
        await expect(errorTip).toBeVisible();
        await expect(errorTip).toHaveText('Dashboard name is required!');
    });

    test('should create dashboard in a folder', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-db-btn');

        await page.fill('#db-name', 'Folder Dashboard');
        await page.click('.folder-select-btn');
        await page.click('.folder-item >> text=Dashboards');

        const navigationPromise = page.waitForResponse((response) => response.url().includes('/api/dashboards/create') && response.status() === 200);

        await page.click('#save-dbbtn');
        await navigationPromise;

        await expect(page).toHaveURL(/.*dashboard\.html\?id=/, { timeout: 10000 });

        const url = page.url();
        createdDashboardIds.push(url.split('id=')[1]);
    });

    test('should cancel dashboard creation', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-db-btn');

        await page.fill('#db-name', 'Test Dashboard');
        await page.click('#cancel-dbbtn');

        // Verify modal is closed
        await expect(page.locator('#new-dashboard-modal')).not.toBeVisible();
        // Verify we're still on the home page
        await expect(page.url()).toContain('dashboards-home.html');
    });

    test('should retain entered data after validation error', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-db-btn');

        const description = 'Test Description';
        await page.fill('#db-description', description);

        // Try to save without name (will show error)
        await page.click('#save-dbbtn');

        // Verify description is still there
        await expect(page.locator('#db-description')).toHaveValue(description);
    });
});
