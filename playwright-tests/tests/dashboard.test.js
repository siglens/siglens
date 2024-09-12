const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    let dashboardId;

    test.beforeAll(async ({ browser }) => {
        const page = await browser.newPage();
        await createDashboard(page);
        await page.close();
    });

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
    };

    test('Dashboard Functionality Tests', async ({ page }) => {
        // Navigate to the created dashboard
        await page.goto(`http://localhost:5122/dashboard.html?id=${dashboardId}`);

        // Verify dashboard loaded correctly
        await expect(page.locator('#new-dashboard')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toBeVisible();
        await expect(page.locator('#panel-container')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toContainText('Test Dashboard Playwright');

        // Create a new panel
        await expect(page.locator('#add-widget-options .editPanelMenu')).toBeVisible();
        await page.click('#add-panel-btn');
        await expect(page.locator('#add-widget-options')).not.toBeVisible();
        await page.click('#add-panel-btn');
        await page.click('.widget-option[data-index="0"]'); // Select Line Chart
        await expect(page.locator('.panelEditor-container')).toBeVisible();
        await page.fill('#panEdit-nameChangeInput', 'Updated Panel Name');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Updated Panel Name');

        const panelHeader = page.locator('.panel-header').first();
        const editIcon = panelHeader.locator('img.panel-edit-li');
        const viewIcon = panelHeader.locator('img.panel-view-li');
        const optionsBtn = panelHeader.locator('#panel-options-btn');

        await panelHeader.hover();
        await expect(editIcon).toBeVisible();
        await expect(viewIcon).toBeVisible();
        await expect(optionsBtn).toBeVisible();

        // Change dashboard settings
        await page.click('#db-settings-btn');
        await expect(page.locator('.dbSet-container')).toBeVisible();
        const updatedName = 'Updated Dashboard Name ' + Date.now();
        await page.fill('.dbSet-dbName', updatedName);
        await page.click('#dbSet-save');
        await page.waitForTimeout(2000);
        await expect(page.locator('.name-dashboard')).toContainText(updatedName);

        // Toggle favorite status
        const initialState = await page.locator('#favbutton').getAttribute('class');
        await page.click('#favbutton');
        const newState = await page.locator('#favbutton').getAttribute('class');
        expect(newState).not.toBe(initialState);

        // Set refresh interval
        await page.click('#refresh-picker-btn');
        const dropdownMenu = page.locator('.refresh-picker');
        await expect(dropdownMenu).toBeVisible();
        const refreshOption = page.locator('.refresh-range-item', { hasText: '5m' });
        await refreshOption.click();
        await expect(refreshOption).toHaveClass(/active/);
        const refreshButtonText = page.locator('#refresh-picker-btn span');
        await expect(refreshButtonText).toHaveText('5m');

        // Test date picker
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