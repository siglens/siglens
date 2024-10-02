const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('PQS Settings Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/pqs-settings.html');

    // Check if PQS toggle button is present and functional
    const toggleButton = page.locator('#contact-types');
    await expect(toggleButton).toBeVisible();
    const initialState = await toggleButton.innerText();

    // Toggle PQS state
    await toggleButton.click();
    const newOption = page.locator('.contact-option:not(.active)');
    await newOption.click();

    // Handle confirmation if disabling
    const disableConfirm = page.locator('#disable-pqs');
    if (await disableConfirm.isVisible()) {
        await disableConfirm.click();
    }

    await page.waitForTimeout(500);

    const newState = await toggleButton.innerText();
    expect(newState.toLowerCase()).not.toBe(initialState.toLowerCase());

    // Verify Clear PQS Meta Data button
    const clearButton = page.locator('#clear-pqs-info');
    await expect(clearButton).toBeVisible();
    await expect(clearButton).toHaveText('Clear PQS Meta Data');

    // Check Promoted Searches and Aggregations sections
    await expect(page.locator('text=Promoted Searches')).toBeVisible();
    await expect(page.locator('text=Promoted Aggregations')).toBeVisible();

    // Verify grid containers are present
    const promotedSearchesGrid = page.locator('#ag-grid-promoted-searches');
    const promotedAggregationsGrid = page.locator('#ag-grid-promoted-aggregations');
    await expect(promotedSearchesGrid).toBeVisible();
    await expect(promotedAggregationsGrid).toBeVisible();

    // Theme Button
    await testThemeToggle(page);
});
