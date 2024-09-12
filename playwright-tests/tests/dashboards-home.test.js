const { test, expect } = require('@playwright/test');

test.describe('Dashboards Home Page', () => {
    test.beforeEach(async ({ page }) => {
        // Navigate to the dashboards home page
        await page.goto('http://localhost:5122/dashboards-home.html');
    });

    test('should load dashboards and display grid', async ({ page }) => {
        // Wait for the grid to be visible
        await expect(page.locator('#dashboard-grid')).toBeVisible();

        // Check if at least one dashboard is displayed
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

        await nameInput.fill('New Test Dashboard');
        await descriptionInput.fill('This is a test dashboard');
        await saveButton.click();

        // Check if redirected to the new dashboard page
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
        // Wait for the grid to load
        await page.waitForSelector('.ag-center-cols-container .ag-row');

        // Click the star icon of the first dashboard
        const starIcon = page.locator('.ag-center-cols-container .ag-row:first-child .star-icon');
        await starIcon.click();

        // Check if the star icon's background image changes
        // Note: This might need adjustment based on how your CSS is implemented
        await expect(starIcon).toHaveCSS('background-image', /star-filled/);
    });

    test('should open delete confirmation for a dashboard', async ({ page }) => {
        // Wait for the grid to load
        await page.waitForSelector('.ag-center-cols-container .ag-row');

        // Click the delete button of the first dashboard that's not a default dashboard
        const deleteButton = page.locator('.ag-center-cols-container .ag-row:not(:has(.default-label)) .btn-simple').first();
        await deleteButton.click();

        await expect(page.locator('#delete-db-prompt')).toBeVisible();
    });
});


