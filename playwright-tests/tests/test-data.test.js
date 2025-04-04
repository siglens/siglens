const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('Test Data Ingestion Page Test', () => {
    test('should have sample-data div with message', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();

        // Wait for page to be fully loaded
        await page.waitForLoadState('networkidle');

        // Click the "Send Test Data" card to make sample-data visible
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Wait for sample-data with a longer timeout
        await page.waitForSelector('#sample-data', { state: 'visible', timeout: 60000 });
        await expect(page.locator('#sample-data')).toBeVisible();
        const sampleDataDiv = page.locator('#sample-data');
        await expect(sampleDataDiv).toBeVisible();
        await expect(sampleDataDiv).toContainText('Get started by sending sample logs data');

        // Theme Button
        await testThemeToggle(page);
    });

    test('check number of ingestion method tabs', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');
        await page.waitForLoadState('networkidle');

        // Update selector to match actual HTML structure
        const listItems = page.locator('.ingestion-card');
        const itemCount = await listItems.count();
        expect(itemCount).toBe(9);
    });

    test('should switch between ingestion methods tabs', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');
        await page.waitForLoadState('networkidle');

        // Click the "Send Test Data" card to make sample-data visible
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Wait for sample-data with a longer timeout
        await page.waitForSelector('#sample-data', { state: 'visible', timeout: 60000 });
        await expect(page.locator('#sample-data')).toBeVisible();
        await expect(page.locator('#data-ingestion')).not.toBeVisible();

        await page.locator('#option-2').click();
        await page.waitForSelector('#data-ingestion', { state: 'visible', timeout: 60000 });
        await expect(page.locator('#data-ingestion')).toBeVisible();
        await expect(page.locator('#sample-data')).not.toBeVisible();
    });

    test('should add test-data', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');
        await page.waitForLoadState('networkidle');

        // Click the "Send Test Data" card to make test-data-btn visible
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Wait for test-data-btn with a longer timeout
        await page.waitForSelector('#test-data-btn', { state: 'visible', timeout: 60000 });
        await page.locator('#test-data-btn').click();
        await page.waitForTimeout(1000);

        const toast = page.locator('#message-toast');
        await expect(toast).toContainText('Sent Test Data Successfully');
    });
});
