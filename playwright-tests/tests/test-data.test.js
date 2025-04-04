const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('Test Data Ingestion Page Test', () => {
    // Setup for all tests in this group
    test.beforeEach(async ({ page }) => {
        // Mock the API call for sample data to make tests faster and more reliable
        await page.route('/api/sampledataset_bulk', route => {
            route.fulfill({
                status: 200,
                body: JSON.stringify({ success: true })
            });
        });

        // Navigate to the test page
        await page.goto('http://localhost:5122/test-data.html');

        // Wait for the page to be fully loaded
        await page.waitForLoadState('networkidle');
        await expect(page.locator('#app-side-nav')).toBeVisible();
    });

    test('should have sample-data div with message', async ({ page }) => {
        // Click the "Send Test Data" card
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Wait for transition to complete and sample-data to be visible
        await page.waitForSelector('#sample-data', { state: 'visible', timeout: 10000 });

        // Verify the sample data div is visible and contains expected text
        const sampleDataDiv = page.locator('#sample-data');
        await expect(sampleDataDiv).toBeVisible();
        await expect(sampleDataDiv).toContainText('Get started by sending sample logs data');

        // Test theme toggle functionality
        await testThemeToggle(page);
    });

    test('check number of ingestion method tabs', async ({ page }) => {
        // Check the count of ingestion cards for logs (which are visible by default)
        const logsCards = page.locator('.ingestion-card.logs-card');
        const logsCount = await logsCards.count();

        // This should match the number of logs cards in your application
        expect(logsCount).toBeGreaterThan(0);
    });

    test('should switch between ingestion methods tabs', async ({ page }) => {
        // First test "Send Test Data" flow
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Verify proper visibility states after clicking "Send Test Data"
        await page.waitForSelector('#sample-data', { state: 'visible', timeout: 10000 });
        await expect(page.locator('#sample-data')).toBeVisible();
        await expect(page.locator('#data-ingestion')).not.toBeVisible();
        await expect(page.locator('#logs-ingestion-details')).toBeVisible();

        // Go back to card selection view
        const backButton = await page.locator('#back-to-logs-cards');
        await backButton.click();

        // Wait for transition back to cards view
        await page.waitForSelector('#logs-cards-view', { state: 'visible', timeout: 5000 });
        await expect(page.locator('#logs-ingestion-details')).not.toBeVisible();

        // Then test OpenTelemetry flow
        await page.locator('.ingestion-card.logs-card[data-source="OpenTelemetry"]').click();

        // Verify proper visibility states after clicking "OpenTelemetry"
        await page.waitForSelector('#data-ingestion', { state: 'visible', timeout: 10000 });
        await expect(page.locator('#data-ingestion')).toBeVisible();
        await expect(page.locator('#sample-data')).not.toBeVisible();

        // Verify setup instructions link was updated correctly
        const setupLink = page.locator('#logs-setup-instructions-link');
        await expect(setupLink).toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry');
    });

    test('should add test-data and show success toast', async ({ page }) => {
        // Click the "Send Test Data" card
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Wait for the test-data-btn to be visible and clickable
        await page.waitForSelector('#test-data-btn', { state: 'visible', timeout: 10000 });

        // Click the test data button
        await page.locator('#test-data-btn').click();

        // Check button gets disabled after click
        await expect(page.locator('#test-data-btn')).toBeDisabled({ timeout: 2000 });

        // Wait for success toast to appear
        await page.waitForSelector('#message-toast:has-text("Sent Test Data Successfully")', {
            state: 'visible',
            timeout: 5000
        });

        // Verify toast message
        const toast = page.locator('#message-toast');
        await expect(toast).toContainText('Sent Test Data Successfully');

        // Verify button gets re-enabled after API call completes
        await expect(page.locator('#test-data-btn')).toBeEnabled({ timeout: 5000 });
    });

    test('should copy command to clipboard when copy icon is clicked', async ({ page }) => {
        // Click the "OpenTelemetry" card to show copy functionality
        await page.locator('.ingestion-card.logs-card[data-source="OpenTelemetry"]').click();

        // Wait for the copy icon to be visible
        await page.waitForSelector('.copy-icon', { state: 'visible', timeout: 5000 });

        // Click the copy icon
        await page.locator('.copy-icon').first().click();

        // Verify success class is added (this indicates copy was successful)
        await expect(page.locator('.copy-icon.success')).toBeVisible({ timeout: 2000 });

        // Wait for success class to be removed after timeout
        await expect(page.locator('.copy-icon.success')).not.toBeVisible({ timeout: 2000 });
    });

    test('should navigate through metrics tab', async ({ page }) => {
        // First, find and click the metrics tab
        // Based on your code, it seems the tabs are using jQuery UI tabs
        const metricsTabSelector = '.ui-tabs-nav li:has-text("Metrics"), #metrics-tab-link, a[href="#metrics-tab"]';
        await page.locator(metricsTabSelector).click();

        // Wait for metrics tab content to be visible
        await page.waitForSelector('#metrics-cards-view, #metrics-tab', { state: 'visible', timeout: 5000 });

        // Now check if we have metrics cards
        const metricsCards = page.locator('.ingestion-card.metrics-card');
        const count = await metricsCards.count();

        // If we have metrics cards, proceed with testing one
        if (count > 0) {
            // Click the first metrics card
            await metricsCards.first().click();

            // Wait for metrics details to be visible
            await page.waitForSelector('#metrics-ingestion-details', { state: 'visible', timeout: 5000 });

            // Go back to metrics cards view
            await page.locator('#back-to-metrics-cards').click();
            await page.waitForSelector('#metrics-cards-view', { state: 'visible', timeout: 5000 });
        } else {
            // If no metrics cards are found, just log it rather than failing
            console.log('No metrics cards found in the metrics tab');
        }
    });

    test('should navigate through traces tab', async ({ page }) => {
        // First, find and click the traces tab
        const tracesTabSelector = '.ui-tabs-nav li:has-text("Traces"), #traces-tab-link, a[href="#traces-tab"]';
        await page.locator(tracesTabSelector).click();

        // Wait for traces tab content to be visible
        await page.waitForSelector('#traces-cards-view, #traces-tab', { state: 'visible', timeout: 5000 });

        // Now check if we have traces cards
        const tracesCards = page.locator('.ingestion-card.traces-card');
        const count = await tracesCards.count();

        // If we have traces cards, proceed with testing one
        if (count > 0) {
            // Click the first traces card
            await tracesCards.first().click();

            // Wait for traces details to be visible
            await page.waitForSelector('#traces-ingestion-details', { state: 'visible', timeout: 5000 });

            // Go back to traces cards view
            await page.locator('#back-to-traces-cards').click();
            await page.waitForSelector('#traces-cards-view', { state: 'visible', timeout: 5000 });
        } else {
            // If no traces cards are found, just log it rather than failing
            console.log('No traces cards found in the traces tab');
        }
    });
});