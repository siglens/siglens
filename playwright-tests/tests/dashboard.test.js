const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    let dashboardId;

    const createDashboard = async (page) => {
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.click('#create-db-btn');
        const uniqueName = `Test Dashboard Playwright ${Date.now()}`;
        await page.fill('#db-name', uniqueName);
        await page.fill('#db-description', 'This is a test dashboard');
        await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle' }), page.click('#save-dbbtn')]);
        const url = page.url();
        dashboardId = url.split('id=')[1];
        if (!dashboardId) throw new Error('Failed to extract dashboard ID from URL');
        await expect(page.locator('#new-dashboard')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toBeVisible();
        await expect(page.locator('#panel-container')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toContainText('Test Dashboard Playwright');
    };

    test.beforeEach(async ({ page }) => {
        await createDashboard(page);
    });

    test('should display the correct dashboard title', async ({ page }) => {
        const title = await page.locator('.name-dashboard').textContent();
        expect(title).toContain('Test Dashboard Playwright');
    });

    test('should load dashboard and display panels', async ({ page }) => {
        await expect(page.locator('#new-dashboard')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toBeVisible();
        await expect(page.locator('#panel-container')).toBeVisible();
    });

    test('should be able to create a new panel', async ({ page }) => {
        await expect(page.locator('#add-widget-options .editPanelMenu')).toBeVisible();
        await page.click('#add-panel-btn');
        await expect(page.locator('#add-widget-options')).not.toBeVisible();
        await page.click('#add-panel-btn');
        await page.click('.widget-option[data-index="0"]'); // Select Line Chart
        await expect(page.locator('.panelEditor-container')).toBeVisible();
        await page.fill('#panEdit-nameChangeInput', 'Updated Panel Name');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Updated Panel Name');
    });

    test('should be able to change dashboard settings', async ({ page }) => {
        await page.click('#db-settings-btn');
        await expect(page.locator('.dbSet-container')).toBeVisible();
        await page.fill('.dbSet-dbName', 'Updated Dashboard Name ' + Date.now());
        await page.click('#dbSet-save');
        await page.waitForTimeout(2000);
        await expect(page.locator('.name-dashboard')).toContainText('Updated Dashboard Name');
    });

    test('should be able to toggle favorite status', async ({ page }) => {
        const initialState = await page.locator('#favbutton').getAttribute('class');
        await page.click('#favbutton');
        const newState = await page.locator('#favbutton').getAttribute('class');
        expect(newState).not.toBe(initialState);
    });

    test('should be able to set refresh interval', async ({ page }) => {
        await page.click('#refresh-picker-btn');
        const dropdownMenu = page.locator('.refresh-picker');
        await expect(dropdownMenu).toBeVisible();
        const refreshOption = page.locator('.refresh-range-item', { hasText: '5m' });
        await refreshOption.click();
        await expect(refreshOption).toHaveClass(/active/);
        const refreshButtonText = page.locator('#refresh-picker-btn span');
        await expect(refreshButtonText).toHaveText('5m');
    });

    test('should open the date picker, select a time range, and verify the button updates', async ({ page }) => {
        await page.click('#new-dashboard #date-picker-btn');
        const datePickerMenu = page.locator('#new-dashboard .daterangepicker');
        await expect(datePickerMenu).toBeVisible();
        const timeRangeOption = page.locator('#new-dashboard #now-5m');
        await timeRangeOption.click();
        await expect(timeRangeOption).toHaveClass(/active/);
        const datePickerButtonText = page.locator('#new-dashboard #date-picker-btn span');
        await expect(datePickerButtonText).toHaveText('Last 5 Mins');
    });
});
