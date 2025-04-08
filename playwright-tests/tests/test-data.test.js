const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('Logs Ingestion Page Tests', () => {
    test('should display logs ingestion cards view by default', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();
        await expect(page.locator('#logs-cards-view')).toBeVisible();
        await expect(page.locator('#sample-data')).toBeVisible();
        await expect(page.locator('#data-ingestion')).not.toBeVisible();

        // Theme Button
        await testThemeToggle(page);
    });

    test('should navigate to log details when a logs card is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');

        // Click on one of the logs ingestion cards (e.g., OpenTelemetry)
        await page.locator('.ingestion-card.logs-card[data-source="OpenTelemetry"]').click();

        // Check that the logs cards view is hidden and details view is shown
        await expect(page.locator('#logs-cards-view')).not.toBeVisible();
        await expect(page.locator('#logs-ingestion-details')).toBeVisible();
        await expect(page.locator('#data-ingestion')).toBeVisible();
        await expect(page.locator('#sample-data')).not.toBeVisible();

        // Check URL parameter was updated
        expect(page.url()).toContain('method=opentelemetry');
    });

    test('should load correct details for Send Test Data card', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html');

        // Click on Send Test Data card
        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        // Check that the logs cards view is hidden and details view is shown
        await expect(page.locator('#logs-cards-view')).not.toBeVisible();
        await expect(page.locator('#logs-ingestion-details')).toBeVisible();

        // For "Send Test Data", data-ingestion should be hidden and sample-data shown
        await expect(page.locator('#data-ingestion')).not.toBeVisible();
        await expect(page.locator('#sample-data')).toBeVisible();

        // Check URL parameter was updated
        expect(page.url()).toContain('method=sendtestdata');
    });

    test('should process URL parameter and show correct logs details', async ({ page }) => {
        // Navigate directly with method parameter
        await page.goto('http://localhost:5122/test-data.html?method=elasticbulk');

        // Check that the logs cards view is hidden and details view is shown
        await expect(page.locator('#logs-cards-view')).not.toBeVisible();
        await expect(page.locator('#logs-ingestion-details')).toBeVisible();
        await expect(page.locator('#data-ingestion')).toBeVisible();
        await expect(page.locator('#sample-data')).not.toBeVisible();

        // Check platform input contains correct value
        await expect(page.locator('#platform-input')).toHaveValue('Elastic Bulk');
    });

    test('should send test data when button is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html?method=sendtestdata');

        // Check that test data button is visible
        const testDataButton = page.locator('#test-data-btn');
        await expect(testDataButton).toBeVisible();

        // Click the button and check it gets disabled
        await testDataButton.click();
        await expect(testDataButton).toBeDisabled();

        // Wait for the toast message
        await page.waitForTimeout(1000);
        const toast = page.locator('#message-toast');
        await expect(toast).toContainText('Sent Test Data Successfully');

        // Button should be re-enabled after success
        await expect(testDataButton).toBeEnabled();
    });
});

test.describe('Metrics Ingestion Page Tests', () => {
    test('should display metrics ingestion cards view by default', async ({ page }) => {
        await page.goto('http://localhost:5122/metrics-ingestion.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();
        await expect(page.locator('#metrics-cards-view')).toBeVisible();

        // Theme Button
        await testThemeToggle(page);
    });

    test('should navigate to metrics details when a metrics card is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/metrics-ingestion.html');

        // Click on one of the metrics ingestion cards (e.g., VectorMetrics)
        await page.locator('.ingestion-card.metrics-card[data-source="VectorMetrics"]').click();

        // Check that the metrics cards view is hidden and details view is shown
        await expect(page.locator('#metrics-cards-view')).not.toBeVisible();
        await expect(page.locator('#metrics-ingestion-details')).toBeVisible();

        // Check URL parameter was updated
        expect(page.url()).toContain('method=vectorMetrics');

        // Check the setup instructions link was updated
        const instructionsLink = page.locator('#metrics-setup-instructions-link');
        await expect(instructionsLink).toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/vector-metrics');
    });

    test('should process URL parameter and show correct metrics details', async ({ page }) => {
        // Navigate directly with method parameter
        await page.goto('http://localhost:5122/metrics-ingestion.html?method=opentelemetry');

        // Check that the metrics cards view is hidden and details view is shown
        await expect(page.locator('#metrics-cards-view')).not.toBeVisible();
        await expect(page.locator('#metrics-ingestion-details')).toBeVisible();

        // Check the setup instructions link was updated
        const instructionsLink = page.locator('#metrics-setup-instructions-link');
        await expect(instructionsLink).toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/open-telemetry');
    });
});

test.describe('Traces Ingestion Page Tests', () => {
    test('should display traces ingestion cards view by default', async ({ page }) => {
        await page.goto('http://localhost:5122/traces-ingestion.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();
        await expect(page.locator('#traces-cards-view')).toBeVisible();

        // Theme Button
        await testThemeToggle(page);
    });

    test('should navigate to traces details when a traces card is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/traces-ingestion.html');

        // Click on one of the traces ingestion cards (e.g., Go App)
        await page.locator('.ingestion-card.traces-card[data-source="Go App"]').click();

        // Check that the traces cards view is hidden and details view is shown
        await expect(page.locator('#traces-cards-view')).not.toBeVisible();
        await expect(page.locator('#traces-ingestion-details')).toBeVisible();

        // Check URL parameter was updated
        expect(page.url()).toContain('method=goApp');

        // Check the setup instructions link was updated
        const instructionsLink = page.locator('#traces-setup-instructions-link');
        await expect(instructionsLink).toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/instrument-traces/go-app');
    });

    test('should process URL parameter and show correct traces details', async ({ page }) => {
        // Navigate directly with method parameter
        await page.goto('http://localhost:5122/traces-ingestion.html?method=javaApp');

        // Check that the traces cards view is hidden and details view is shown
        await expect(page.locator('#traces-cards-view')).not.toBeVisible();
        await expect(page.locator('#traces-ingestion-details')).toBeVisible();

        // Check the setup instructions link was updated
        const instructionsLink = page.locator('#traces-setup-instructions-link');
        await expect(instructionsLink).toHaveAttribute('href', 'https://www.siglens.com/siglens-docs/instrument-traces/java-app');
    });
});

test.describe('Common Functionality Tests', () => {
    test('should copy content when copy icon is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/test-data.html?method=opentelemetry');

        // Find a copyable element and its adjacent copy icon
        const copyIcon = page.locator('.copy-icon').first();
        await expect(copyIcon).toBeVisible();

        // Click the copy icon and check it gets the success class
        await copyIcon.click();
        await expect(copyIcon).toHaveClass(/success/);

        // Success class should be removed after 1 second
        await page.waitForTimeout(1100);
        await expect(copyIcon).not.toHaveClass(/success/);
    });

    test('should update breadcrumbs when navigating to details view', async ({ page }) => {
        // Test breadcrumbs on logs page
        await page.goto('http://localhost:5122/test-data.html');
        await page.locator('.ingestion-card.logs-card[data-source="OpenTelemetry"]').click();

        // Check that breadcrumbs are updated
        const breadcrumbs = page.locator('.breadcrumb');
        await expect(breadcrumbs).toContainText('Log Ingestion Methods');
        await expect(breadcrumbs).toContainText('OpenTelemetry');

        // Test breadcrumbs on metrics page
        await page.goto('http://localhost:5122/metrics-ingestion.html');
        await page.locator('.ingestion-card.metrics-card[data-source="VectorMetrics"]').click();

        // Check that breadcrumbs are updated
        await expect(breadcrumbs).toContainText('Metrics Ingestion Methods');
        await expect(breadcrumbs).toContainText('VectorMetrics');

        // Test breadcrumbs on traces page
        await page.goto('http://localhost:5122/traces-ingestion.html');
        await page.locator('.ingestion-card.traces-card[data-source="Python App"]').click();

        // Check that breadcrumbs are updated
        await expect(breadcrumbs).toContainText('Traces Ingestion Methods');
        await expect(breadcrumbs).toContainText('Python App');
    });
});