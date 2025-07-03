const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test.describe('Logs Ingestion Page Tests', () => {
    test('should display logs ingestion cards view by default', async ({ page }) => {
        await page.goto('http://localhost:5122/log-ingestion.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();
        await expect(page.locator('#logs-cards-view')).toBeVisible();

        await page.waitForTimeout(1000);

        const sampleDataExists = (await page.locator('#sample-data').count()) > 0;
        if (!sampleDataExists) {
            console.log('Warning: #sample-data element not found');
        }

        if ((await page.locator('#data-ingestion').count()) > 0) {
            await expect(page.locator('#data-ingestion')).not.toBeVisible();
        }

        await testThemeToggle(page);
    });

    test('should navigate to log details when a logs card is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/log-ingestion.html');
        await page.waitForTimeout(1000);

        await page.locator('.ingestion-card.logs-card[data-source="OpenTelemetry Collector"]').click();

        await page.waitForTimeout(1000);

        await expect(page.locator('#logs-cards-view')).not.toBeVisible();
        await expect(page.locator('#content-container')).toBeVisible();

        expect(page.url()).toContain('method=opentelemetry');
    });

    test('should load correct details for Send Test Data card', async ({ page }) => {
        await page.goto('http://localhost:5122/log-ingestion.html');
        await page.waitForTimeout(1000);

        await page.locator('.ingestion-card.logs-card[data-source="Send Test Data"]').click();

        await page.waitForTimeout(1000);

        await expect(page.locator('#logs-cards-view')).not.toBeVisible();
        await expect(page.locator('#logs-ingestion-details')).toBeVisible();

        expect(page.url()).toContain('method=sendtestdata');
    });

    test('should process URL parameter and show correct logs details', async ({ page }) => {
        await page.goto('http://localhost:5122/log-ingestion.html?method=sendtestdata');
        await page.waitForTimeout(1000); 

        await expect(page.locator('#logs-cards-view')).not.toBeVisible();
        await expect(page.locator('#logs-ingestion-details')).toBeVisible();
    });

    test('should send test data when button is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/log-ingestion.html?method=sendtestdata');
        await page.waitForTimeout(1000); 

        const testDataButton = page.locator('#test-data-btn');
        await expect(testDataButton).toBeVisible();

        await testDataButton.click();
        await expect(testDataButton).toBeDisabled();

        await page.waitForTimeout(2000);
        const toast = page.locator('#message-toast');
        await expect(toast).toContainText('Sent Test Data Successfully');

        await expect(testDataButton).toBeEnabled();
    });
});

test.describe('Metrics Ingestion Page Tests', () => {
    test('should display metrics ingestion cards view by default', async ({ page }) => {
        await page.goto('http://localhost:5122/metrics-ingestion.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();
        await expect(page.locator('#metrics-cards-view')).toBeVisible();
        await page.waitForTimeout(1000); 

        if ((await page.locator('#data-ingestion').count()) > 0) {
            await expect(page.locator('#data-ingestion')).not.toBeVisible();
        }

        await testThemeToggle(page);
    });

    test('should navigate to metrics details when a metrics card is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/metrics-ingestion.html');
        await page.waitForTimeout(1000); 

        await page.locator('.ingestion-card.metrics-card[data-source="Vector Metrics"]').click();

        await page.waitForTimeout(1000);

        await expect(page.locator('#metrics-cards-view')).not.toBeVisible();
        await expect(page.locator('#content-container')).toBeVisible();

        expect(page.url()).toContain('method=vector-metrics');

    });
});

test.describe('Traces Ingestion Page Tests', () => {
    test('should display traces ingestion cards view by default', async ({ page }) => {
        await page.goto('http://localhost:5122/traces-ingestion.html');
        await expect(page.locator('#app-side-nav')).toBeVisible();
        await expect(page.locator('#traces-cards-view')).toBeVisible();

        await page.waitForTimeout(1000); 

        if ((await page.locator('#data-ingestion').count()) > 0) {
            await expect(page.locator('#data-ingestion')).not.toBeVisible();
        }

        await testThemeToggle(page);
    });

    test('should navigate to traces details when a traces card is clicked', async ({ page }) => {
        await page.goto('http://localhost:5122/traces-ingestion.html');
        await page.waitForTimeout(1000); 

        await page.locator('.ingestion-card.traces-card[data-source="Go App"]').click();

        await page.waitForTimeout(1000);

        await expect(page.locator('#traces-cards-view')).not.toBeVisible();
        await expect(page.locator('#content-container')).toBeVisible();

        expect(page.url()).toContain('method=go-app');

    });

});
