const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    let page;
    let dashboardId;
    let uniqueName;

    test.beforeEach(async ({ browser }) => {
        page = await browser.newPage();
        
        // Create dashboard
        await page.goto('http://localhost:5122/dashboards-home.html');
        await page.click('#create-db-btn');
        uniqueName = `Test Dashboard Playwright ${Date.now()}`;
        await page.fill('#db-name', uniqueName);
        await page.fill('#db-description', 'This is a test dashboard');
        await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle' }), page.click('#save-dbbtn')]);
        const url = page.url();
        dashboardId = url.split('id=')[1];
        if (!dashboardId) throw new Error('Failed to extract dashboard ID from URL');

        // Navigate to the created dashboard
        await page.goto(`http://localhost:5122/dashboard.html?id=${dashboardId}`);

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

    test('Verify dashboard loads correctly', async () => {
        await expect(page.locator('#new-dashboard')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toBeVisible();
        await expect(page.locator('#panel-container')).toBeVisible();
        await expect(page.locator('.name-dashboard')).toContainText(uniqueName);
    });

    test('Edit panel', async () => {
        const panelHeader = page.locator('.panel-header').first();
        const editIcon = panelHeader.locator('img.panel-edit-li');
        
        await panelHeader.hover();
        await expect(editIcon).toBeVisible();
        await editIcon.click();
        
        const editPanel = page.locator('.panelEditor-container');
        await expect(editPanel).toBeVisible();
        await page.fill('#panEdit-nameChangeInput', 'Updated Panel Name');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Updated Panel Name');
    });

    test('View panel', async () => {
        const panelHeader = page.locator('.panel-header').first();
        const viewIcon = panelHeader.locator('img.panel-view-li');
        
        await panelHeader.hover();
        await expect(viewIcon).toBeVisible();
        await viewIcon.click();
        
        const viewPanel = page.locator('#viewPanel-container');
        await expect(viewPanel).toBeVisible();
        await expect(page.locator('#overview-button')).toHaveClass(/active/);
        
        const editButton = page.locator('#edit-button');
        await editButton.click();
        await expect(page.locator('#edit-button')).toHaveClass(/active/);
        
        const cancelButton = page.locator('.panelEditor-container #discard-btn');
        await expect(cancelButton).toBeVisible();
        await cancelButton.click();
        await expect(page.locator('.panelEditor-container')).not.toBeVisible();
        await expect(page.locator('#panel-container')).toBeVisible();
    });

    test('Delete panel', async () => {
        const panelHeader = page.locator('.panel-header').first();
        const optionsBtn = panelHeader.locator('#panel-options-btn');
        
        const initialPanelCount = await page.locator('.panel').count();
        await panelHeader.hover();
        await expect(optionsBtn).toBeVisible();
        await optionsBtn.click();
        
        const dropdownMenu = page.locator('#panel-dropdown-modal');
        await expect(dropdownMenu).toBeVisible();
        const deleteOption = dropdownMenu.locator('.panel-remove-li');
        await expect(deleteOption).toBeVisible();
        await deleteOption.click();
        
        const deleteConfirmDialog = page.locator('#panel-del-prompt');
        await expect(deleteConfirmDialog).toBeVisible();
        const confirmDeleteBtn = deleteConfirmDialog.locator('#delete-btn-panel');
        await expect(confirmDeleteBtn).toBeVisible();
        await confirmDeleteBtn.click();
        
        await page.waitForTimeout(2000);
        const finalPanelCount = await page.locator('.panel').count();
        expect(finalPanelCount).toBe(initialPanelCount - 1);
    });

    test('Change dashboard settings', async () => {
        await page.click('#db-settings-btn');
        await expect(page.locator('.dbSet-container')).toBeVisible();
        const updatedName = 'Updated Dashboard Name ' + Date.now();
        await page.fill('.dbSet-dbName', updatedName);
        await page.click('#dbSet-save');
        await page.waitForTimeout(2000);
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
});