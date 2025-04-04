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
        // Check the count of ingestion cards
        const listItems = page.locator('.ingestion-card');
        const itemCount = await listItems.count();
        expect(itemCount).toBe(9);
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

    test('should test metrics and traces card navigation', async ({ page }) => {
        // First check if the element exists and log what's available
        const metricsCards = await page.locator('.ingestion-card.metrics-card').all();
        console.log('Available metrics cards:', await Promise.all(metricsCards.map(el => el.getAttribute('data-source'))));

        // Find the OpenTelemetry metrics card regardless of case
        const openTelemetryCard = page.locator('.ingestion-card.metrics-card').filter({
            hasText: /OpenTelemetry|Opentelemetry/i
        });

        // Make sure we found the card
        await expect(openTelemetryCard).toBeVisible();

        // Click the card
        await openTelemetryCard.click();

        // Continue with the test
        await page.waitForSelector('#metrics-ingestion-details', { state: 'visible', timeout: 10000 });
        await expect(page.locator('#metrics-setup-instructions-link'))
            .toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/open-telemetry');

        // Go back to metrics cards view
        await page.locator('#back-to-metrics-cards').click();
        await page.waitForSelector('#metrics-cards-view', { state: 'visible', timeout: 10000 });

        // Test traces card navigation - same approach, find by text
        const pythonAppCard = page.locator('.ingestion-card.traces-card').filter({
            hasText: 'Python App'
        });

        await expect(pythonAppCard).toBeVisible();
        await pythonAppCard.click();

        await page.waitForSelector('#traces-ingestion-details', { state: 'visible', timeout: 10000 });
        await expect(page.locator('#traces-setup-instructions-link'))
            .toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/instrument-traces/python-app');

        // Go back to traces cards view
        await page.locator('#back-to-traces-cards').click();
        await page.waitForSelector('#traces-cards-view', { state: 'visible', timeout: 10000 });
    });
});