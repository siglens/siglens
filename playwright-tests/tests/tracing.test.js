const { test, expect } = require('@playwright/test');
const { testDateTimePicker, testThemeToggle } = require('./common-functions');

test('Service Health Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/service-health.html');

    // Check for the presence of key elements
    await expect(page.locator('#app-side-nav')).toBeVisible();
    await expect(page.locator('#dashboard')).toBeVisible();
    await expect(page.locator('#app-footer')).toBeVisible();

    // Check the date picker
    await testDateTimePicker(page);

    // Check the search input
    const searchInput = page.locator('.search-input');
    await expect(searchInput).toBeVisible();
    await searchInput.fill('test search');
    await expect(searchInput).toHaveValue('test search');

    // Check the ag-grid
    await expect(page.locator('#ag-grid')).toBeVisible();

    // Test theme toggle
    await testThemeToggle(page);
});

test('Search Traces Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/search-traces.html');

    // Check for the presence of key elements
    await expect(page.locator('#app-side-nav')).toBeVisible();
    await expect(page.locator('#dashboard')).toBeVisible();
    await expect(page.locator('#app-footer')).toBeVisible();

    // Check the search section
    await expect(page.locator('.search')).toBeVisible();
    await expect(page.locator('#service-dropdown')).toBeVisible();
    await expect(page.locator('#name-dropdown')).toBeVisible();
    await expect(page.locator('#tags-input')).toBeVisible();
    await expect(page.locator('#min-duration-input')).toBeVisible();
    await expect(page.locator('#max-duration-input')).toBeVisible();
    await expect(page.locator('#limit-result-input')).toBeVisible();
    await expect(page.locator('#search-trace-btn')).toBeVisible();

    // Check the right section
    await expect(page.locator('.right-section')).toBeVisible();
    await expect(page.locator('#traces-number')).toBeVisible();
    await expect(page.locator('#sort-dropdown')).toBeVisible();
    await expect(page.locator('#download-dropdown')).toBeVisible();
    await expect(page.locator('#graph-show')).toBeVisible();

    // Test search button click
    await page.click('#search-trace-btn');

    // Test sort dropdown
    const sortDropdown = page.locator('#sort-dropdown');
    await expect(sortDropdown).toBeVisible();
    await expect(sortDropdown.locator('#mostrecent-span-name')).toHaveText('Most Recent');
    await sortDropdown.locator('#mostrecent-btn').click();
    await expect(sortDropdown.locator('#mostrecent-options')).toBeVisible();
    await sortDropdown.locator('text=Longest First').click();
    await expect(sortDropdown.locator('#mostrecent-span-name')).toHaveText('Longest First');
    await expect(sortDropdown.locator('#mostrecent-options')).toBeHidden();

    // Test download dropdown
    const downloadDropdown = page.locator('#download-dropdown');
    await expect(downloadDropdown).toBeVisible();
    await downloadDropdown.locator('#downloadresult-btn').click();
    await expect(downloadDropdown.locator('#downloadresult-options')).toBeVisible();

    // Test service dropdown
    await page.click('#service-dropdown .service-btn');
    await expect(page.locator('#service-dropdown #service-options')).toBeVisible();
    await page.click('#service-dropdown .service-btn');
    await expect(page.locator('#service-dropdown #service-options')).not.toBeVisible();

    // Test operation dropdown
    await page.click('#name-dropdown .operation-btn');
    await expect(page.locator('#name-dropdown #operation-options')).toBeVisible();
    await page.click('#name-dropdown .operation-btn');
    await expect(page.locator('#name-dropdown #operation-options')).not.toBeVisible();

    // // Test lookback dropdown
    await testDateTimePicker(page);

    // Test input fields
    await page.fill('#tags-input', 'test tag');
    await expect(page.locator('#tags-input')).toHaveValue('test tag');

    await page.fill('#min-duration-input', '100ms');
    await expect(page.locator('#min-duration-input')).toHaveValue('100ms');

    await page.fill('#max-duration-input', '1s');
    await expect(page.locator('#max-duration-input')).toHaveValue('1s');

    await page.fill('#limit-result-input', '50');
    await expect(page.locator('#limit-result-input')).toHaveValue('50');

    // Test theme toggle
    await testThemeToggle(page);
});

test('Dependency Graph Page Test', async ({ page }) => {
    await page.goto('http://localhost:5122/dependency-graph.html');

    // Check for the presence of key elements
    await expect(page.locator('#app-side-nav')).toBeVisible();
    await expect(page.locator('#dashboard')).toBeVisible();
    await expect(page.locator('#app-footer')).toBeVisible();

    // Check the date picker
    const datePickerBtn = page.locator('#date-picker-btn');
    await expect(datePickerBtn).toBeVisible();
    await datePickerBtn.click();
    await expect(page.locator('.daterangepicker')).toBeVisible();

    const timeRangeOption = page.locator('#now-6h');
    await timeRangeOption.click();
    await expect(timeRangeOption).toHaveClass(/active/);
    const datePickerButtonText = page.locator('#date-picker-btn span');
    await expect(datePickerButtonText).toHaveText('Last 6 Hrs');

    // Check for the error message container (since there is no dependency graph)
    await expect(page.locator('#error-msg-container')).toBeVisible();

    // Test theme toggle
    await testThemeToggle(page);
});
