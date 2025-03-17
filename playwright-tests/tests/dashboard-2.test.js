const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    test.setTimeout(90000);
    let uniqueName;
    let createdDashboardIds = [];

    test('Create dashboard with panel', async ({ page }) => {
        // Create dashboard
        await page.goto('http://localhost:5122/dashboards-home.html', {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await page.click('#add-new-container .dropdown .btn');
        await expect(page.locator('#add-new-container .dropdown .dropdown-menu')).toBeVisible({ timeout: 15000 });

        await page.click('#create-db-btn');
        await expect(page.locator('#new-dashboard-modal')).toBeVisible({ timeout: 15000 });
        uniqueName = `Test Dashboard Playwright ${Date.now()}`;

        await page.fill('#db-name', uniqueName);

        const navigationPromise = page.waitForResponse((response) => response.url().includes('/api/dashboards/create') && response.status() === 200, { timeout: 30000 });

        await page.click('#save-dbbtn');
        await navigationPromise;

        await expect(page).toHaveURL(/.*dashboard\.html\?id=/, { timeout: 30000 });

        const url = page.url();
        const dashboardId = url.split('id=')[1];
        createdDashboardIds.push(dashboardId);

        // Create a new panel
        await expect(page.locator('#add-widget-options .editPanelMenu')).toBeVisible({ timeout: 15000 });
        await page.click('#add-panel-btn');
        await expect(page.locator('#add-widget-options')).not.toBeVisible({ timeout: 15000 });
        await page.click('#add-panel-btn');

        await expect(page.locator('.widget-option[data-index="0"]')).toBeVisible({ timeout: 15000 });
        await page.click('.widget-option[data-index="0"]'); // Select Line Chart

        await expect(page.locator('.panelEditor-container')).toBeVisible({ timeout: 15000 });
        await page.fill('#panEdit-nameChangeInput', 'Test Panel');
        await page.click('.panEdit-save');

        await expect(page.locator('.panel-header p')).toContainText('Test Panel', { timeout: 15000 });
    });

    test('Change dashboard settings', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });
        await page.waitForTimeout(2000);

        await page.click('#db-settings-btn');
        await expect(page.locator('.dbSet-container')).toBeVisible({ timeout: 15000 });

        const updatedName = 'Updated Dashboard Name ' + Date.now();
        await page.fill('.dbSet-dbName', updatedName);

        await page.waitForTimeout(1000);

        const responsePromise = page.waitForResponse((response) => response.url().includes('/api/dashboards/') && response.status() === 200, { timeout: 30000 });
        await page.click('#dbSet-save');
        await responsePromise;

        await page.waitForTimeout(2000);
        await expect(page.locator('.name-dashboard')).toContainText(updatedName, { timeout: 15000 });
    });

    test('Toggle favorite status', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });
        await page.waitForTimeout(2000);

        await page.waitForSelector('#favbutton', { state: 'visible', timeout: 15000 });
        const initialState = await page.locator('#favbutton').getAttribute('class');

        await page.click('#favbutton');

        await page.waitForTimeout(3000);

        const newState = await page.locator('#favbutton').getAttribute('class');
        expect(newState).not.toBe(initialState);
    });

    test('Set refresh interval', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });
        await page.waitForTimeout(2000);

        await page.click('#refresh-picker-btn');
        const refreshDropdownMenu = page.locator('.refresh-picker');
        await expect(refreshDropdownMenu).toBeVisible({ timeout: 15000 });

        const refreshOption = page.locator('.refresh-range-item', { hasText: '5m' });
        await refreshOption.click();

        await page.waitForTimeout(1000);
        await expect(refreshOption).toHaveClass(/active/, { timeout: 15000 });

        const refreshButtonText = page.locator('#refresh-picker-btn span');
        await expect(refreshButtonText).toHaveText('5m', { timeout: 15000 });
    });

    test('Test date picker', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });
        await page.waitForTimeout(2000);

        await page.click('#new-dashboard #date-picker-btn');
        const datePickerMenu = page.locator('#new-dashboard .daterangepicker');
        await expect(datePickerMenu).toBeVisible({ timeout: 15000 });

        const timeRangeOption = page.locator('#new-dashboard #now-5m');
        await timeRangeOption.click();

        await page.waitForTimeout(1000);
        await expect(timeRangeOption).toHaveClass(/active/, { timeout: 15000 });

        const datePickerButtonText = page.locator('#new-dashboard #date-picker-btn span');
        await expect(datePickerButtonText).toHaveText('Last 5 Mins', { timeout: 15000 });
    });

    test.afterAll(async ({ browser }) => {
        const cleanupPage = await browser.newPage();

        try {
            // Delete dashboards using API calls
            for (const id of createdDashboardIds) {
                try {
                    const response = await cleanupPage.request.get(`http://localhost:5122/api/dashboards/delete/${id}`, {
                        headers: {
                            'Content-Type': 'application/json',
                            Accept: '*/*',
                        },
                        timeout: 30000,
                    });

                    if (!response.ok()) {
                        console.error(`Failed to delete dashboard ${id}: ${response.statusText()}`);
                    }
                } catch (error) {
                    console.error(`Error deleting dashboard ${id}:`, error);
                }
            }
        } finally {
            await cleanupPage.close();
        }
    });
});
