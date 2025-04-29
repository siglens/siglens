const { test, expect } = require('@playwright/test');

test.describe('Dashboard Page Tests', () => {
    test.setTimeout(120000);
    let createdDashboardIds = [];

    test('Create dashboard with panel and verify panels', async ({ page }) => {
        await page.goto('http://localhost:5122/dashboards-home.html', {
            timeout: 60000,
            waitUntil: 'networkidle',
        });

        await page.click('#add-new-container .dropdown .btn');
        await expect(page.locator('#add-new-container .dropdown .dropdown-menu')).toBeVisible({ timeout: 20000 });

        await page.click('#create-db-btn');
        await expect(page.locator('#new-dashboard-modal')).toBeVisible({ timeout: 20000 });
        const uniqueName = `Test Dashboard ${Date.now()}`;

        await page.fill('#db-name', uniqueName);

        const navigationPromise = page.waitForResponse((response) => response.url().includes('/api/dashboards/create') && response.status() === 200, { timeout: 45000 });
        await page.click('#save-dbbtn');
        await navigationPromise;

        await page.waitForURL(/.*dashboard\.html\?id=/, { timeout: 45000 });
        await page.waitForLoadState('networkidle', { timeout: 45000 });

        const url = page.url();
        const dashboardId = url.split('id=')[1];
        createdDashboardIds.push(dashboardId);

        await page.waitForSelector('#add-widget-options .editPanelMenu', { timeout: 30000 });
        await page.click('#add-panel-btn');
        await page.waitForTimeout(1000);

        if (await page.locator('#add-widget-options').isVisible()) {
            await page.waitForTimeout(1000);
        } else {
            await page.click('#add-panel-btn');
        }

        await page.waitForSelector('.widget-option[data-index="0"]', { timeout: 30000 });
        await page.click('.widget-option[data-index="0"]'); // Line Chart

        await page.waitForSelector('.panelEditor-container', { timeout: 30000 });
        await page.waitForSelector('#panEdit-nameChangeInput', { timeout: 15000 });
        await page.fill('#panEdit-nameChangeInput', 'Test Panel');
        await page.click('.panEdit-save');

        await page.waitForSelector('.panel-header p', { timeout: 30000 });

        await expect(page.locator('#new-dashboard')).toBeVisible({ timeout: 15000 });
        await expect(page.locator('.name-dashboard')).toBeVisible({ timeout: 15000 });
        await expect(page.locator('#panel-container')).toBeVisible({ timeout: 15000 });
        await expect(page.locator('.panel-header')).toBeVisible({ timeout: 15000 });

        // Edit panel
        const panelHeader = page.locator('.panel-header').first();
        await page.waitForFunction(() => {
            const header = document.querySelector('.panel-header');
            const emptyResponse = document.querySelector('#empty-response');
            return header && !emptyResponse;
        }, {}, { timeout: 30000 });
        await panelHeader.hover({ timeout: 15000 }); 
        await page.waitForTimeout(500); 

        const editIcon = panelHeader.locator('img.panel-edit-li');
        await expect(editIcon).toBeVisible({ timeout: 15000 });
        await editIcon.click();

        await expect(page.locator('.panelEditor-container')).toBeVisible({ timeout: 20000 });
        await page.waitForTimeout(2000);

        await page.fill('#panEdit-nameChangeInput', 'Updated Panel Name');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Updated Panel Name', { timeout: 20000 });

        // View panel
        await panelHeader.hover();
        await page.waitForTimeout(1000);

        const viewIcon = panelHeader.locator('img.panel-view-li');
        await expect(viewIcon).toBeVisible({ timeout: 15000 });
        await viewIcon.click();

        await expect(page.locator('#viewPanel-container')).toBeVisible({ timeout: 20000 });
        await page.waitForTimeout(2000);

        await page.click('#discard-btn');
        // Delete panel

        await panelHeader.hover();
        await page.waitForTimeout(1000);

        const optionsBtn = panelHeader.locator('#panel-options-btn');
        await expect(optionsBtn).toBeVisible({ timeout: 15000 });
        await optionsBtn.click();

        await expect(page.locator('#panel-dropdown-modal')).toBeVisible({ timeout: 15000 });
        const deleteOption = page.locator('.panel-remove-li');
        await deleteOption.click();

        await expect(page.locator('#panel-del-prompt')).toBeVisible({ timeout: 15000 });
        await page.click('#delete-btn-panel');
        await page.waitForTimeout(3000);
    });

    test('Dashboard UI functions', async ({ page }) => {
        // Use the ID from first test
        if (createdDashboardIds.length === 0) {
            test.skip('No dashboard ID available');
        }

        await page.goto(`http://localhost:5122/dashboard.html?id=${createdDashboardIds[0]}`, {
            timeout: 60000,
            waitUntil: 'networkidle',
        });

        await page.waitForSelector('#new-dashboard', { timeout: 30000 });
        await page.waitForTimeout(2000);

        // Toggle favorite
        await page.waitForSelector('#favbutton', { state: 'visible', timeout: 20000 });
        const favButton = page.locator('#favbutton').first();
        await favButton.click();
        await page.waitForTimeout(3000);

        // Test date picker
        const datePickerBtn = page.locator('#new-dashboard #date-picker-btn');
        await datePickerBtn.click();

        const dateRangePicker = page.locator('#new-dashboard .daterangepicker').first();
        await expect(dateRangePicker).toBeVisible({ timeout: 15000 });

        const timeRangeOption = dateRangePicker.locator('#now-5m');
        await timeRangeOption.click();
        await page.waitForTimeout(2000);

        const datePickerText = datePickerBtn.locator('span');
        await expect(datePickerText).toHaveText('Last 5 Mins', { timeout: 15000 });

        // Change dashboard settings
        await page.click('#db-settings-btn');
        await expect(page.locator('.dbSet-container')).toBeVisible({ timeout: 20000 });

        const updatedName = 'Updated Dashboard ' + Date.now();
        await page.fill('.dbSet-dbName', updatedName);
        await page.waitForTimeout(1000);
        await page.click('#dbSet-save');
    });

    test.afterAll(async ({ browser }) => {
        const cleanupPage = await browser.newPage();
        try {
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
                        console.log(`Failed to delete dashboard ${id}: ${response.statusText()}`);
                    }
                } catch (error) {
                    console.log(`Error deleting dashboard ${id}: ${error.message}`);
                }
            }
        } finally {
            await cleanupPage.close();
        }
    });
});
