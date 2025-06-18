const { test, expect } = require('@playwright/test');
const { testThemeToggle } = require('./common-functions');

test('Navigation Menu Part 2', async ({ page }) => {
    page.setDefaultNavigationTimeout(60000);

    await page.goto('http://localhost:5122/index.html');

    const navItems = [
        { selector: '.nav-usq', url: 'saved-queries.html' },
        { selector: '.nav-myorg', url: 'cluster-stats.html' },
        { selector: '.nav-lookups', url: 'lookups.html' },
    ];

    for (const { selector, url } of navItems) {
        const navbarHamburger = page.locator('#navbar-toggle').first();
        await navbarHamburger.hover();
        await page.waitForTimeout(1000);

        await Promise.all([
            page.waitForNavigation({ waitUntil: 'domcontentloaded' }),
            page.click(`${selector} a`)
        ]);

        await expect(page.locator(selector)).toHaveClass(/active/, { timeout: 15000 });

        if (url === 'cluster-stats.html') {
            await expect(page.locator('.org-nav-tab')).toBeVisible({ timeout: 15000 });
        }
    }

    // Test metrics pages
    const metricsPages = ['metrics-explorer.html', 'metric-summary.html', 'metric-cardinality.html'];
    for (const url of metricsPages) {
        await page.goto(`http://localhost:5122/${url}`, {
            waitUntil: 'domcontentloaded',
            timeout: 45000
        });
        await page.waitForTimeout(1000);
        await expect(page.locator('.nav-metrics')).toHaveClass(/active/, { timeout: 15000 });
    }

    // Test org pages
    const orgPages = ['cluster-stats.html', 'org-settings.html', 'application-version.html', 'pqs-settings.html', 'license.html'];
    for (const url of orgPages) {
        await page.goto(`http://localhost:5122/${url}`, {
            waitUntil: 'domcontentloaded',
            timeout: 45000
        });
        await page.waitForTimeout(1000);
        await expect(page.locator('.nav-myorg')).toHaveClass(/active/, { timeout: 15000 });
    }

    // Test Infrastructure pages
    const infrastructurePages = [
        'kubernetes-overview.html',
        'kubernetes-view.html?type=clusters',
        'kubernetes-view.html?type=namespaces',
        'kubernetes-view.html?type=workloads',
        'kubernetes-view.html?type=nodes',
        'kubernetes-view.html?type=events',
        'kubernetes-view.html?type=configuration'
    ];
    for (const url of infrastructurePages) {
        await page.goto(`http://localhost:5122/${url}`, {
            waitUntil: 'domcontentloaded',
            timeout: 45000
        });
        await page.waitForTimeout(1000);
        await expect(page.locator('.nav-infrastructure')).toHaveClass(/active/, { timeout: 15000 });

    }

    // Theme button
    await testThemeToggle(page);
});