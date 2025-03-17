const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    test.setTimeout(90000);

    let uniqueName;
    let createdDashboardNames = [];
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

        createdDashboardNames.push(uniqueName);
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

        await expect(page.locator('#new-dashboard')).toBeVisible({ timeout: 15000 });
        await expect(page.locator('.name-dashboard')).toBeVisible({ timeout: 15000 });
        await expect(page.locator('#panel-container')).toBeVisible({ timeout: 15000 });
        await expect(page.locator('.name-dashboard')).toContainText(uniqueName, { timeout: 15000 });
    });

    test('Edit panel', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });

        await page.waitForTimeout(2000);

        const panelHeader = page.locator('.panel-header').first();
        const editIcon = panelHeader.locator('img.panel-edit-li');

        await panelHeader.hover();
        await expect(editIcon).toBeVisible({ timeout: 15000 });
        await editIcon.click();

        const editPanel = page.locator('.panelEditor-container');
        await expect(editPanel).toBeVisible({ timeout: 15000 });

        await page.waitForTimeout(1000);

        await page.fill('#panEdit-nameChangeInput', 'Updated Panel Name');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Updated Panel Name', { timeout: 15000 });
    });

    test('View panel', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });

        await page.waitForTimeout(2000);

        const panelHeader = page.locator('.panel-header').first();
        const viewIcon = panelHeader.locator('img.panel-view-li');

        await panelHeader.hover();
        await expect(viewIcon).toBeVisible({ timeout: 15000 });
        await viewIcon.click();

        const viewPanel = page.locator('#viewPanel-container');
        await expect(viewPanel).toBeVisible({ timeout: 15000 });
        await expect(page.locator('#overview-button')).toHaveClass(/active/, { timeout: 15000 });

        await page.waitForTimeout(2000);

        const editButton = page.locator('#edit-button');
        await editButton.click();
        await expect(page.locator('#edit-button')).toHaveClass(/active/, { timeout: 15000 });

        await page.waitForTimeout(2000);

        const cancelButton = page.locator('.panelEditor-container #discard-btn');
        await expect(cancelButton).toBeVisible({ timeout: 15000 });
        await cancelButton.click();
        await expect(page.locator('.panelEditor-container')).not.toBeVisible({ timeout: 15000 });
        await expect(page.locator('#panel-container')).toBeVisible({ timeout: 15000 });
    });

    test('Delete panel', async ({ page }) => {
        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            waitUntil: 'domcontentloaded',
            timeout: 60000,
        });

        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 30000 });

        await page.waitForTimeout(2000);

        const panelHeader = page.locator('.panel-header').first();
        const optionsBtn = panelHeader.locator('#panel-options-btn');

        const initialPanelCount = await page.locator('.panel').count();

        await panelHeader.hover();
        await expect(optionsBtn).toBeVisible({ timeout: 15000 });
        await optionsBtn.click();

        const dropdownMenu = page.locator('#panel-dropdown-modal');
        await expect(dropdownMenu).toBeVisible({ timeout: 15000 });

        const deleteOption = dropdownMenu.locator('.panel-remove-li');
        await expect(deleteOption).toBeVisible({ timeout: 15000 });
        await deleteOption.click();

        const deleteConfirmDialog = page.locator('#panel-del-prompt');
        await expect(deleteConfirmDialog).toBeVisible({ timeout: 15000 });

        const confirmDeleteBtn = deleteConfirmDialog.locator('#delete-btn-panel');
        await expect(confirmDeleteBtn).toBeVisible({ timeout: 15000 });
        await confirmDeleteBtn.click();

        await page.waitForTimeout(5000);

        const finalPanelCount = await page.locator('.panel').count();
        expect(finalPanelCount).toBe(initialPanelCount - 1);
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
        createdDashboardIds = [];
    });
});
