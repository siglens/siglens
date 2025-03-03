const { expect } = require('@playwright/test');

async function testThemeToggle(page) {
    const navbarHamburger = page.locator('.sl-hamburger');
    await navbarHamburger.hover();

    await page.waitForTimeout(300);
    
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

module.exports = {
    testDateTimePicker,
    testThemeToggle,
};