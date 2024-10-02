const { test, expect } = require('@playwright/test');
const { testDateTimePicker, testThemeToggle } = require('./common-functions');

test.describe('Saved Queries Tests', () => {
    test('Save query, open it, and delete it', async ({ page }) => {
        // Save a query
        await page.goto('http://localhost:5122/index.html');
        await testDateTimePicker(page);
        await page.locator('#query-builder-btn').click();
        await page.waitForTimeout(1000);
        await expect(page.locator('#logs-result-container')).toBeVisible();

        await page.click('#saveq-btn');

        const dialog = page.locator('[aria-describedby="save-queries"]');
        await dialog.waitFor({ state: 'visible' });

        await page.fill('[aria-describedby="save-queries"] #qname', 'Test-Query');
        await page.fill('[aria-describedby="save-queries"] #description', 'This is a test query');

        const saveButton = dialog.locator('.saveqButton');
        await saveButton.waitFor({ state: 'visible' });
        await saveButton.click();

        const toast = page.locator('#message-toast');
        await expect(toast).toContainText('Query saved successfully');

        // Open saved-queries.html and verify the saved query
        await page.goto('http://localhost:5122/saved-queries.html');

        // Wait for the ag-grid to load
        await page.waitForSelector('.ag-root-wrapper');

        // Check if the saved query is present in the grid
        const cellWithTestQuery = page.locator('.ag-cell-value:has-text("Test-Query")');
        await expect(cellWithTestQuery).toBeVisible();

        // Click on the saved query
        await cellWithTestQuery.click();

        // Verify that it opens the index page with the table visible
        await expect(page).toHaveURL(/index\.html/);
        await expect(page.locator('#logs-result-container')).toBeVisible();

        // Go back to saved queries page
        await page.goto('http://localhost:5122/saved-queries.html');

        // Wait for the ag-grid to load again
        await page.waitForSelector('.ag-root-wrapper');

        // Find and click the delete button for the Test-Query
        const deleteButton = page.locator('.ag-row:has-text("Test-Query") .btn-simple');
        await deleteButton.click();

        // Confirm deletion in the popup
        await page.locator('#delete-btn').click();

        // Verify that the query is no longer in the grid
        await expect(cellWithTestQuery).not.toBeVisible();

        // Theme Button
        await testThemeToggle(page);
    });
});
