const { test, expect } = require('@playwright/test');

test('Contact Point Test', async ({ page }) => {
    
    await page.goto('http://localhost:5122/contacts.html');

    await page.click('#new-contact-point');

    const contactName = `Test Contact ${Date.now()}`;
    await page.fill('#contact-name', contactName);
    await page.fill('#slack-channel-id', 'test-channel');
    await page.fill('#slack-token', 'xoxb-test-token');

    // Add a new contact type (Webhook)
    await page.click('.add-new-contact-type');
    await page.click('.contact-container:nth-child(2) #contact-types');
    await page.click('.contact-container:nth-child(2) .contact-option:text("Webhook")');
    await page.fill('.contact-container:nth-child(2) #webhook-id', 'https://test-webhook.com');

    await page.click('#save-contact-btn');

    await page.waitForURL('**/contacts.html');

    // Verify the new contact point appears in the AG Grid
    const contactRow = await page.locator('.ag-center-cols-container .ag-row:last-child');
    const newContactName = await contactRow.locator('[col-id="contactName"]').textContent();
    expect(newContactName).toContain('Test Contact');

    const contactType = await contactRow.locator('[col-id="type"]').textContent();
    expect(contactType).toBe('Slack, Webhook');

    // Click the edit button
    await contactRow.locator('#editbutton').first().click();
    await expect(page.locator('#contact-form-container')).toBeVisible();

    // Update the contact name
    const updatedContactName = `Updated Test Contact ${Date.now()}`;
    await page.fill('#contact-name', updatedContactName);

    await page.click('#save-contact-btn');

    await page.waitForURL('**/contacts.html');

    // Wait for the grid to update
    await page.waitForTimeout(1000);

    // Verify the updated contact name appears in the table
    const updatedContactRow = await page.locator('.ag-center-cols-container .ag-row:has-text("' + updatedContactName + '")');
    expect(await updatedContactRow.locator('[col-id="contactName"]').textContent()).toContain(updatedContactName);

    // Delete the contact point
    await updatedContactRow.locator('#delbutton').first().click();

    await expect(page.locator('.popupContent.active')).toBeVisible();

    const confirmationText = await page.locator('#contact-name-placeholder strong').textContent();
    expect(confirmationText).toContain(updatedContactName); // Verify the contact name in the confirmation popup

    await page.click('#delete-btn');

    // Verify the contact has been removed from the table
    await expect(updatedContactRow).toBeHidden();
});