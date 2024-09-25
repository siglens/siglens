const { test, expect } = require('@playwright/test');
const { createAlert } = require('./common-functions');

test.describe('Alert Tests', () => {
    test('Create a new metrics alert', async ({ page }) => {
        await createAlert(page, 'Metrics', 'option-2');
    });
});
