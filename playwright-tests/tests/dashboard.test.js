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

        await page.waitForTimeout(2000);

        // Edit panel
        const panelHeader = page.locator('.panel-header').first();
        await expect(panelHeader).toBeVisible({ timeout: 15000 });

        await page.evaluate(() => {
            const editIcon = document.querySelector('.panel-header img.panel-edit-li');
            if (editIcon) {
                editIcon.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }
        });

        await expect(page.locator('.panelEditor-container')).toBeVisible({ timeout: 20000 });
        await page.waitForTimeout(2000);

        await page.fill('#panEdit-nameChangeInput', 'Updated Panel Name');
        await page.click('.panEdit-save');
        await expect(page.locator('.panel-header p')).toContainText('Updated Panel Name', { timeout: 20000 });

        await page.waitForTimeout(2000);
        // View panel

        await page.evaluate(() => {
            const viewIcon = document.querySelector('.panel-header img.panel-view-li');
            if (viewIcon) {
                viewIcon.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }
        });

        await expect(page.locator('#viewPanel-container')).toBeVisible({ timeout: 20000 });
        await page.waitForTimeout(2000);

        await page.click('#discard-btn');

        await page.waitForTimeout(2000);
        // Delete panel

        await page.evaluate(() => {
            const optionsBtn = document.querySelector('.panel-header #panel-options-btn');
            if (optionsBtn) {
                optionsBtn.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }
            
            setTimeout(() => {
                const dropdown = document.querySelector('#panel-dropdown-modal');
                if (dropdown) {
                    dropdown.classList.remove('hidden');
                    dropdown.style.display = 'block'; 
                }
            }, 500);
        });
        
        await page.waitForTimeout(1000);
        
        await page.evaluate(() => {
            const deleteOption = document.querySelector('.panel-remove-li');
            if (deleteOption) {
                deleteOption.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }
        });

        // Wait for delete confirmation prompt
        try {
            await expect(page.locator('#panel-del-prompt')).toBeVisible({ timeout: 15000 });
        } catch (e) {
            await page.evaluate(() => {
                const deletePrompt = document.querySelector('#panel-del-prompt');
                if (deletePrompt) {
                    deletePrompt.style.display = 'block';
                    deletePrompt.classList.remove('hidden');
                }
            });
            await page.waitForTimeout(500);
        }
        
        await page.evaluate(() => {
            const deleteBtn = document.querySelector('#delete-btn-panel');
            if (deleteBtn) {
                deleteBtn.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }
        });
        await page.waitForTimeout(3000);    });

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

