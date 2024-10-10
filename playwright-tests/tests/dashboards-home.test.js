const { test, expect } = require('@playwright/test');

test.describe('Dashboards Home Page', () => {
    test.beforeEach(async ({ page }) => {
        // Navigate to the dashboards home page
        await page.goto('http://localhost:5122/dashboards-home.html');
        
        await expect(page.locator('#dashboard-grid')).toBeVisible();
    });

    test('should load dashboards and display grid', async ({ page }) => {
        const dashboardRows = page.locator('.ag-center-cols-container .ag-row');
        await expect(dashboardRows.first()).toBeVisible();
    });

    test('should open create dashboard modal', async ({ page }) => {
        const createButton = page.locator('#create-db-btn');
        await createButton.click();

        await expect(page.locator('#new-dashboard-modal')).toBeVisible();
    });

    test('should create a new dashboard', async ({ page }) => {
        const createButton = page.locator('#create-db-btn');
        await createButton.click();

        const nameInput = page.locator('#db-name');
        const descriptionInput = page.locator('#db-description');
        const saveButton = page.locator('#save-dbbtn');

        await nameInput.fill('New Test Dashboard' + Date.now());
        await descriptionInput.fill('This is a test dashboard');
        await saveButton.click();

        await expect(page).toHaveURL(/.*dashboard\.html\?id=/);
    });

    test('should show error for empty dashboard name', async ({ page }) => {
        const createButton = page.locator('#create-db-btn');
        await createButton.click();

        const saveButton = page.locator('#save-dbbtn');
        await saveButton.click();

        await expect(page.locator('.error-tip')).toBeVisible();
        await expect(page.locator('.error-tip')).toHaveText('Dashboard name is required!');
    });

    test('should toggle favorite status of a dashboard', async ({ page }) => {
        await page.waitForSelector('.ag-center-cols-container .ag-row');

        const starIcon = page.locator('.ag-center-cols-container .ag-row:first-child .star-icon');

        const getBackgroundImage = async () => starIcon.evaluate((el) => window.getComputedStyle(el).backgroundImage);
        const initialBackgroundImage = await getBackgroundImage();

        await starIcon.click();

        // Wait for the background image to change
        await expect(async () => {
            const newBackgroundImage = await getBackgroundImage();
            expect(newBackgroundImage).not.toBe(initialBackgroundImage);
        }).toPass({timeout: 5000});
    });

    test('should open delete confirmation for a dashboard', async ({ page }) => {
        await page.waitForSelector('.ag-center-cols-container .ag-row');

        const deleteButton = page.locator('.ag-center-cols-container .ag-row:not(:has(.default-label)) .btn-simple').first();
        await deleteButton.click();

        await expect(page.locator('#delete-db-prompt')).toBeVisible();
    });
});
