const { test, expect } = require('@playwright/test');
const { createLogsAlert } = require('./common-functions');

test('Create a new alert', async ({ page }) => {
    await createLogsAlert(page);
});
