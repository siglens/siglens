const { test, expect } = require('@playwright/test');
const { createAlert } = require('./common-functions');

test.describe('Alert Tests', () => {
    test('Create a new logs alert', async ({ page }) => {
        await createAlert(page, 'Logs', 'option-1', 'option-1', 'city=Boston | stats count AS Count BY weekday');
    });
});
