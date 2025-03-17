const { test, expect } = require('@playwright/test');

test('Navigation Menu Part 1', async ({ page }) => {
    page.setDefaultNavigationTimeout(60000);
    
    await page.goto('http://localhost:5122/index.html');
    await expect(page.locator('.nav-search')).toHaveClass(/active/);

    const navItems = [
        { selector: '.nav-search', url: 'index.html' },
        { selector: '.nav-slos', url: 'all-slos.html' },
        { selector: '.nav-alerts', url: 'all-alerts.html' },
        { selector: '.nav-ldb', url: 'dashboards-home.html' },
        { selector: '.nav-minion', url: 'minion-searches.html' },
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

        if (url === 'all-alerts.html') {
            await expect(page.locator('.alerts-nav-tab')).toBeVisible({ timeout: 15000 });
        }
    }

    // Test tracing pages
    const tracingPages = ['service-health.html', 'search-traces.html', 'dependency-graph.html'];
    for (const url of tracingPages) {
        await page.goto(`http://localhost:5122/${url}`, { 
            waitUntil: 'domcontentloaded',
            timeout: 45000
        });
        await page.waitForTimeout(1000);
        await expect(page.locator('.nav-traces')).toHaveClass(/active/, { timeout: 15000 });
    }
});