const { By, Key, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const chrome = require('selenium-webdriver/chrome');
let driver;
const chromeOptions = new chrome.Options();
chromeOptions.addArguments('--headless');

async function testContactsPage() {
    try {
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();
        await driver.get("http://localhost/contacts.html");

        // Wait for the "Add Contact Point" button to be visible and clickable
        let addContactButton = await driver.findElement(By.id("new-contact-point"));

        // Test adding a new contact point
        console.log("Before clicking addContactButton");
        await addContactButton.click();
        console.log("After clicking addContactButton");

        // Wait for the contact form to be displayed
        let IscontactFormDisplayed = await driver.findElement(By.id("contact-form-container")).isDisplayed();
        assert.equal(IscontactFormDisplayed, true, 'Contact form container is not displayed after clicking "Add Contact Point"');
    
        let contactNameInput = await driver.findElement(By.id("contact-name"));
        await contactNameInput.sendKeys("ContactNameTest");
        let contactNameValue = await contactNameInput.getAttribute('value');
        assert.equal(contactNameValue, "ContactNameTest", "Contact name input value is not correct");
    
        let contactTypeDropdown = await driver.findElement(By.id("contact-types"));
        await contactTypeDropdown.click();
        let slackOption = await driver.findElement(By.id("option-0"));
        await slackOption.click();
    
        let slackChannelInput = await driver.findElement(By.id("slack-channel-id"));
        await slackChannelInput.sendKeys("070");
        let slackChannelValue = await slackChannelInput.getAttribute('value');
        assert.equal(slackChannelValue, "070", "Slack channel ID input value is not correct");
    
        // Test Slack Token Input
        let slackTokenInput = await driver.findElement(By.id("slack-token"));
        await slackTokenInput.sendKeys("TestTocken");
        let slackTokenValue = await slackTokenInput.getAttribute('value');
        assert.equal(slackTokenValue,"TestTocken", "Slack token input value is not correct");
    
        // Test Save Button Functionality
        let saveContactButton = await driver.findElement(By.id("save-contact-btn"));
        await saveContactButton.click();        
        // Test deleting a contact point
        // Assuming there's a contact point already added
        let deleteButton = await driver.findElement(By.id("delete-btn"));
        console.log("Before clicking deleteButton");

        // Output the current state of the deleteButton
        console.log("Is deleteButton displayed?", await deleteButton.isDisplayed());
        console.log("Is deleteButton enabled?", await deleteButton.isEnabled());

        // Use JavaScript to click the delete button
        await driver.executeScript("arguments[0].click();", deleteButton);

        console.log("After clicking deleteButton");

        // Wait for the confirmation prompt to be present
        await driver.wait(until.elementLocated(By.id('cancel-btn')), 20000);

        // Use JavaScript to click the cancel button without waiting for visibility
        await driver.executeScript("document.getElementById('cancel-btn').click();");

        console.log("All Contacts Page tests passed");
    } catch (err) {
        console.error(err);
    } finally {
        // Close the browser
        await driver.quit();
    }
}

// Run the tests
testContactsPage();
