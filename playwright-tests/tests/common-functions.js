const { expect } = require('@playwright/test');

async function testThemeToggle(page) {
    const themeBtn = page.locator('#theme-btn');
    const html = page.locator('html');
    const initialTheme = await html.getAttribute('data-theme');
    await themeBtn.click();
    expect(await html.getAttribute('data-theme')).not.toBe(initialTheme);
    await themeBtn.click();
    expect(await html.getAttribute('data-theme')).toBe(initialTheme);
}

async function testDateTimePicker(page) {
    // Check the date picker
    const datePickerBtn = page.locator('#date-picker-btn');
    await expect(datePickerBtn).toBeVisible();
    await datePickerBtn.click();
    await expect(page.locator('.daterangepicker')).toBeVisible();

    // Check custom date range inputs
    await expect(page.locator('#date-start')).toBeVisible();
    await expect(page.locator('#time-start')).toBeVisible();
    await expect(page.locator('#date-end')).toBeVisible();
    await expect(page.locator('#time-end')).toBeVisible();
    // Apply custom date range
    await expect(page.locator('#customrange-btn')).toBeVisible();

    // Predefined date range inputs
    await page.click('#date-picker-btn');
    await datePickerBtn.click();
    await expect(page.locator('.daterangepicker')).toBeVisible();
    const timeRangeOption = page.locator('#now-24h');
    await timeRangeOption.click();
    await expect(timeRangeOption).toHaveClass(/active/);
    const datePickerButtonText = page.locator('#date-picker-btn span');
    await expect(datePickerButtonText).toHaveText('Last 24 Hrs');
}

async function createAlert(page, alertType, dataSourceOption, queryLanguageOption = null, query = null) {
    // Navigate to the alerts page
    await page.goto('http://localhost:5122/alert.html');

    // Wait for the alert page to load
    await page.waitForSelector('#alert-rule-name', { state: 'visible' });

    // Fill out the alert form
    await page.fill('#alert-rule-name', `Test Alert ${Date.now()}`);

    // Select data source (Logs or Metrics)
    await page.click('#alert-data-source');
    await page.click(`#data-source-options #${dataSourceOption}`);

    // If there's a specific query language to select (for Logs)
    if (queryLanguageOption) {
        await page.click('#logs-language-btn');
        await page.click(`#logs-language-options #${queryLanguageOption}`);
    }

    // If a query needs to be entered (for Logs)
    if (query) {
        await page.click('#tab-title2'); // Switch to Code tab
        await page.fill('#filter-input', query);
        await page.click('#run-filter-btn'); // Run search
    }

    // Set time range to "Last 30 minutes"
    await page.click('#date-picker-btn');
    await page.click('#now-30m');

    // Set alert condition (Is above)
    await page.click('#alert-condition');
    await page.click('.alert-condition-options #option-0'); // Select "Is above"
    await page.fill('#threshold-value', '100');

    // Set evaluation interval
    await page.fill('#evaluate-every', '5');
    await page.fill('#evaluate-for', '10');

    // Open contact point dropdown
    await page.click('#contact-points-dropdown');

    // Add new contact point (Slack)
    await page.click('.contact-points-options li:nth-child(1)'); // Select the "Add New" option

    // Wait for the contact form popup to appear
    await page.waitForSelector('#add-new-contact-popup', { state: 'visible' });

    // Fill out the contact form (Slack)
    await page.fill('#contact-name', 'Test Contact');
    await page.click('#contact-types'); // Open the type dropdown
    await page.click('.contact-options #option-0'); // Select "Slack"

    // Fill out Slack details
    await page.fill('#slack-channel-id', 'test-channel-id');
    await page.fill('#slack-token', 'xoxb-your-slack-token');

    // Save the contact point
    await page.click('#save-contact-btn');

    // Fill notification message
    await page.fill('#notification-msg', 'This is a test alert notification.');

    // Add a custom label
    await page.click('.add-label-container');
    await page.fill('.label-container #label-key', 'TestLabel');
    await page.fill('.label-container #label-value', 'TestValue');

    // Save the alert
    await page.click('#save-alert-btn');

    // Wait for navigation to the all-alerts page
    await page.waitForNavigation({ url: /all-alerts\.html$/ });

    // Verify that we're on the all-alerts page
    expect(page.url()).toContain('all-alerts.html');
}

module.exports = {
    testDateTimePicker,
    testThemeToggle,
    createAlert,
};
