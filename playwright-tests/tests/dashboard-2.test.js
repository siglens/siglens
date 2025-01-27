const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    let page;
    let uniqueName;
    let createdDashboardIds = [];

    test.beforeEach(async ({ browser }) => {
        page = await browser.newPage();

        // Create dashboard
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.click('#add-new-container .dropdown .btn');
        await expect(page.locator('#add-new-container .dropdown .dropdown-menu ')).toBeVisible();

        await page.click('#create-db-btn');
        await expect(page.locator('#new-dashboard-modal')).toBeVisible();
        uniqueName = `Test Dashboard Playwright ${Date.now()}`;

        await page.fill('#db-name', uniqueName);

        const navigationPromise = page.waitForResponse((response) => response.url().includes('/api/dashboards/create') && response.status() === 200);
        await page.click('#save-dbbtn');
        await navigationPromise;

        await expect(page).toHaveURL(/.*dashboard\.html\?id=/, { timeout: 10000 });

        const url = page.url();
        createdDashboardIds.push(url.split('id=')[1]);

        // Create a new panel
        await expect(page.locator('#add-widget-options .editPanelMenu')).toBeVisible();
        await page.click('#add-panel-btn');
        await expect(page.locator('#add-widget-options')).not.toBeVisible();
        await page.click('#add-panel-btn');
        await page.click('.widget-option[data-index="0"]'); // Select Line Chart
        await expect(page.locator('.panelEditor-container')).toBeVisible();
        await page.fill('#panEdit-nameChangeInput', 'Test Panel');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Test Panel');
    });

    test('Change dashboard settings', async () => {
        await page.click('#db-settings-btn');
        await expect(page.locator('.dbSet-container')).toBeVisible();

        const updatedName = 'Updated Dashboard Name ' + Date.now();
        await page.fill('.dbSet-dbName', updatedName);

        await Promise.all([page.waitForResponse((response) => response.url().includes('/api/dashboards/') && response.status() === 200), page.click('#dbSet-save')]);

        await expect(page.locator('.name-dashboard')).toContainText(updatedName);
    });

    test('Toggle favorite status', async () => {
        await page.waitForSelector('#favbutton', { state: 'visible' });
        const initialState = await page.locator('#favbutton').getAttribute('class');
        await page.click('#favbutton');
        await page.waitForFunction(
            (selector, initialState) => {
                const button = document.querySelector(selector);
                return button && button.getAttribute('class') !== initialState;
            },
            '#favbutton',
            initialState
        );
        const newState = await page.locator('#favbutton').getAttribute('class');
        expect(newState).not.toBe(initialState);
    });

    test('Set refresh interval', async () => {
        await page.click('#refresh-picker-btn');
        const refreshDropdownMenu = page.locator('.refresh-picker');
        await expect(refreshDropdownMenu).toBeVisible();
        const refreshOption = page.locator('.refresh-range-item', { hasText: '5m' });
        await refreshOption.click();
        await expect(refreshOption).toHaveClass(/active/);
        const refreshButtonText = page.locator('#refresh-picker-btn span');
        await expect(refreshButtonText).toHaveText('5m');
    });

    test('Test date picker', async () => {
        await page.click('#new-dashboard #date-picker-btn');
        const datePickerMenu = page.locator('#new-dashboard .daterangepicker');
        await expect(datePickerMenu).toBeVisible();
        const timeRangeOption = page.locator('#new-dashboard #now-5m');
        await timeRangeOption.click();
        await expect(timeRangeOption).toHaveClass(/active/);
        const datePickerButtonText = page.locator('#new-dashboard #date-picker-btn span');
        await expect(datePickerButtonText).toHaveText('Last 5 Mins');
    });

    test.afterEach(async () => {
        await page.close();
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
        createdDashboardIds = [];
    });
});
