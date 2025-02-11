import { test, expect } from '@playwright/test';

test.describe('Create Folder Tests', () => {
    let createdFoldersIds = [];

    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:5122/dashboards-home.html');
    });

    test.afterEach(async ({ page }) => {
        // Clean up any dashboards created during tests
        for (const folderId of createdFoldersIds) {
            await page.goto('http://localhost:5122/dashboards-home.html');

            // Find and delete the dashboard
            try {
                await page.click('#delbutton');
                await page.fill('.confirm-input', 'Delete');
                await page.click('.delete-btn');
            } catch (error) {
                console.log(`Failed to delete dashboard ${folderId}: ${error.message}`);
            }
        }
        createdFoldersIds = [];
    });

    test('should create new folder', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await expect(page.locator('#add-new-container .dropdown .dropdown-menu ')).toBeVisible();

        await page.click('#create-folder-btn');
        await expect(page.locator('#new-folder-modal')).toBeVisible();

        await page.fill('#folder-name', 'Test Folder');

        const navigationPromise = page.waitForResponse((response) => response.url().includes('api/dashboards/folders/create') && response.status() === 200);

        await page.click('#save-folder-btn');
        await navigationPromise;

        const url = page.url();
        createdFoldersIds.push(url.split('id=')[1]);
        await expect(page).toHaveURL(/.*folder\.html\?id=/, { timeout: 10000 });
    });

    test('should validate empty folder name', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-folder-btn');

        await page.click('#save-folder-btn');

        // Try to save without entering name
        const errorTip = page.locator('#new-folder-modal .error-tip');
        await expect(errorTip).toBeVisible();
        await expect(errorTip).toHaveText('Folder name is required!');
    });


    test('should cancel folder creation', async ({ page }) => {
        await page.click('#add-new-container .dropdown .btn');
        await page.click('#create-folder-btn');

        await page.fill('#folder-name', 'Test Folder');
        await page.click('#new-folder-modal #cancel-dbbtn');

        // Verify modal is closed
        await expect(page.locator('#new-folder-modal')).not.toBeVisible();
        // Verify we're still on the home page
        await expect(page.url()).toContain('dashboards-home.html');
    });

});
