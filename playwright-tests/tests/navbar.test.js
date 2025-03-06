const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('Navigation Menu Functionality Tests', async ({ page }) => {
    await page.goto('http://localhost:5122/index.html');
    await expect(page.locator('.nav-search')).toHaveClass(/active/);


    // Test all main navigation items
    const navItems = [
        { selector: '.nav-search', url: 'index.html' },
        { selector: '.nav-metrics', url: 'metrics-explorer.html' },
        { selector: '.nav-slos', url: 'all-slos.html' },
        { selector: '.nav-alerts', url: 'all-alerts.html' },
        { selector: '.nav-ldb', url: 'dashboards-home.html' },
        { selector: '.nav-minion', url: 'minion-searches.html' },
        { selector: '.nav-usq', url: 'saved-queries.html' },
        { selector: '.nav-myorg', url: 'cluster-stats.html' },
        { selector: '.nav-lookups', url: 'lookups.html' },
        { selector: '.nav-ingest', url: 'test-data.html' },
    ];

    for (const { selector, url } of navItems) {
        const navbarHamburger = page.locator('.sl-hamburger');
        await navbarHamburger.hover();
        await page.waitForTimeout(500);

        await page.click(`${selector} a`);
        expect(page.url()).toContain(url);
        await expect(page.locator(selector)).toBeVisible({ timeout: 10000 });
        await expect(page.locator(selector)).toHaveClass(/active/, { timeout: 10000 });

        if (url === 'all-alerts.html') {
            await expect(page.locator('.alerts-nav-tab')).toBeVisible();
        } else if (url === 'cluster-stats.html') {
            await expect(page.locator('.org-nav-tab')).toBeVisible();
        }

        if (['cluster-stats.html', 'all-alerts.html', 'service-health.html'].includes(url)) {
            const upperNavTabs = await page.locator('.subsection-navbar a').all();
            for (const tab of upperNavTabs) {
                await expect(tab).toBeVisible();
            }
        }
    }

    // Test tracing pages
    const tracingPages = ['service-health.html', 'search-traces.html', 'dependency-graph.html'];
    for (const url of tracingPages) {
        await page.goto(`http://localhost:5122/${url}`);
        await expect(page.locator('.nav-traces')).toHaveClass(/active/);
        await expect(page.locator('.subsection-navbar')).toBeVisible();
    }

    // Test metrics pages
    const metricsPages = ['metrics-explorer.html', 'metric-summary.html', 'metric-cardinality.html'];
    for (const url of metricsPages) {
        await page.goto(`http://localhost:5122/${url}`);
        await expect(page.locator('.nav-metrics')).toHaveClass(/active/);
    }

    // Test org pages
    const orgPages = ['cluster-stats.html', 'org-settings.html', 'application-version.html', 'pqs-settings.html'];
    for (const url of orgPages) {
        await page.goto(`http://localhost:5122/${url}`);
        await expect(page.locator('.nav-myorg')).toHaveClass(/active/);
        await expect(page.locator('.org-nav-tab')).toBeVisible();
    }

    //Theme button
    await testThemeToggle(page);
});
