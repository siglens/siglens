const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    let page;
    let uniqueName;
    let createdDashboardNames = [];
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
        createdDashboardNames.push(uniqueName);
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
