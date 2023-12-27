const { By, Key, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const chrome = require("selenium-webdriver/chrome");
let driver;
const chromeOptions = new chrome.Options();

chromeOptions.addArguments("--headless");

async function testContactsPage() {
  try {
    driver = await new Builder()
      .forBrowser("chrome")
      .setChromeOptions(chromeOptions)
      .build();

    await driver.get("http://localhost:5122/contacts.html");

    let alertingHeader = await driver.findElement(By.css(".myOrg-heading")).getText();
    assert.equal(alertingHeader, "Alerting", "Alerting header text is not correct");

    let addContactButton = await driver.findElement(By.id("new-contact-point"));
    await addContactButton.click();

    let contactFormDisplayed = await driver.findElement(By.id("contact-form-container")).isDisplayed();
    assert.equal(contactFormDisplayed, true, 'Contact form container is not displayed after clicking "Add Contact Point"');

    // Additional Tests for the Contact Form
    let contactNameInput = await driver.findElement(By.id("contact-name"));
    await contactNameInput.sendKeys("Test Contact Name");
    let contactNameValue = await contactNameInput.getAttribute('value');
    assert.equal(contactNameValue, "Test Contact Name", "Contact name input value is not correct");

    // Test Dropdown for Contact Types
    let contactTypeDropdown = await driver.findElement(By.id("contact-types"));
    await contactTypeDropdown.click();
    let slackOption = await driver.findElement(By.id("option-0"));
    await slackOption.click();

    // Test Slack Channel ID Input
    let slackChannelInput = await driver.findElement(By.id("slack-channel-id"));
    await slackChannelInput.sendKeys("123456");
    let slackChannelValue = await slackChannelInput.getAttribute('value');
    assert.equal(slackChannelValue, "123456", "Slack channel ID input value is not correct");

    // Test Slack Token Input
    let slackTokenInput = await driver.findElement(By.id("slack-token"));
    await slackTokenInput.sendKeys("Token123");
    let slackTokenValue = await slackTokenInput.getAttribute('value');
    assert.equal(slackTokenValue, "Token123", "Slack token input value is not correct");

    // Test Save Button Functionality
    let saveContactButton = await driver.findElement(By.id("save-contact-btn"));
    await saveContactButton.click();

    await driver.get("http://localhost:5122/contacts.html");

    await driver.sleep(10000); // 5000 milliseconds = 5 seconds
 

     let addedContact = await driver.findElement(By.xpath("//span[contains(@class, 'ag-cell-value')]"));
     let isContactAdded = await addedContact.isDisplayed();
     assert.equal(isContactAdded, true, "New contact was not added to the table");

    console.log("All contacts page tests passed");
  } catch (err) {
    console.error(err);
  } finally {
    await driver.quit();
  }
}

testContactsPage();
