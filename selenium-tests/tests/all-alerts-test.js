const { By, Key, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const chrome = require("selenium-webdriver/chrome");
let driver;
const chromeOptions = new chrome.Options();

chromeOptions.addArguments("--headless");

async function testAllAlertsPage() {
  try {
    driver = await new Builder()
      .forBrowser("chrome")
      .setChromeOptions(chromeOptions)
      .build();

    await driver.get("http://localhost/all-alerts.html");

    // Test for presence of alerting header
    let alertingHeader = await driver.findElement(By.css(".myOrg-heading")).getText();
    assert.equal(alertingHeader, "Alerting", "Alerting header text is not correct");

    // Test for presence of "Add Alert Rule" button
    let addAlertButton = await driver.findElement(By.id("new-alert-rule"));
    let addAlertButtonText = await addAlertButton.getText();
    assert.equal(addAlertButtonText.includes("Add Alert Rule"), true, '"Add Alert Rule" button text is not correct');

    // Check if the alert grid container is displayed
    let alertGridContainerDisplayed = await driver.findElement(By.id("alert-grid-container")).isDisplayed();
    assert.equal(alertGridContainerDisplayed, true, "Alert grid container is not displayed");

    
    console.log("All alerts page tests passed");
  } catch (err) {
    console.error(err);
  } finally {
    await driver.quit();
  }
}

testAllAlertsPage();
