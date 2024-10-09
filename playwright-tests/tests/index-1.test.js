const { test, expect } = require('@playwright/test');
const { testDateTimePicker, testThemeToggle } = require('./common-functions');

test.describe('Logs Page Tests', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:5122/index.html');

        // Perform search to show results table
        await testDateTimePicker(page);
        await page.locator('#query-builder-btn').click();
        await page.waitForTimeout(1000);
        await expect(page.locator('#logs-result-container')).toBeVisible();
    });

    test('verify date picker functionality', async ({ page }) => {
        await testDateTimePicker(page);
    });

    test('verify search and show records functionality', async ({ page }) => {
        await testDateTimePicker(page);

        const searchButton = page.locator('#query-builder-btn');

        await searchButton.click();
        await page.waitForTimeout(1000);
        await expect(page.locator('#logs-result-container')).toBeVisible();

        const showRecordsBtn = page.locator('#show-record-intro-btn');
        await expect(showRecordsBtn).toBeVisible();

        await showRecordsBtn.click();
        await expect(page.locator('div[aria-describedby="show-record-popup"]')).toBeVisible();

        const cancelRecordsBtn = page.locator('.cancel-record-btn');

        await cancelRecordsBtn.click();
        await expect(page.locator('div[aria-describedby="show-record-popup"]')).not.toBeVisible();
    });

    test('should switch between Builder and Code tabs', async ({ page }) => {
        await expect(page.locator('#tabs-1')).toBeVisible();
        await expect(page.locator('#tabs-2')).not.toBeVisible();

        await page.click('#tab-title2');
        await expect(page.locator('#tabs-1')).not.toBeVisible();
        await expect(page.locator('#tabs-2')).toBeVisible();

        await page.click('#tab-title1');
        await expect(page.locator('#tabs-1')).toBeVisible();
        await expect(page.locator('#tabs-2')).not.toBeVisible();
    });

    test('should open and close settings', async ({ page }) => {
        const settingsContainer = page.locator('#setting-container');
        const settingsButton = page.locator('#logs-settings');

        await expect(settingsContainer).not.toBeVisible();
        await settingsButton.click();
        await expect(settingsContainer).toBeVisible();
        await settingsButton.click();
        await expect(settingsContainer).not.toBeVisible();
    });

    test('should change query language', async ({ page }) => {
        await page.click('#logs-settings');
        await page.click('#query-language-btn');
        await page.click('#option-1');
        await expect(page.locator('#query-language-btn span')).toHaveText('SQL');
    });

    test('should change query mode', async ({ page }) => {
        await page.click('#logs-settings');
        await page.click('#query-mode-btn');
        await page.click('#mode-option-2');
        await expect(page.locator('#query-mode-btn span')).toHaveText('Code');
    });

    test('should change theme', async ({ page }) => {
        await testThemeToggle(page);
    });

    test('should change table view', async ({ page }) => {
        await page.click('#log-opt-single-btn');
        await page.click('#log-opt-multi-btn');
        await page.click('#log-opt-table-btn');
    });
});
