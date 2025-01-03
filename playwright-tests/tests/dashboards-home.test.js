const { test, expect } = require('@playwright/test');

test.describe('Dashboards Home Page', () => {
    test.beforeEach(async ({ page }) => {
        // Navigate to the dashboards home page
        await page.goto('http://localhost:5122/dashboards-home.html');
        
        await expect(page.locator('#dashboard-grid')).toBeVisible();
    });

    test('should load dashboards and display grid', async ({ page }) => {
        const dashboardRows = page.locator('.ag-center-cols-container .ag-row');
        await expect(dashboardRows.first()).toBeVisible();
    });
});
