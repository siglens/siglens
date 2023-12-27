const { By, Key, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const fs =require("fs");
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

    await driver.get("http://localhost:5122/all-alerts.html");

    // Test presence and click of "Add Alert Rule" button
    let addAlertRuleButton = await driver.findElement(By.id("new-alert-rule"));
    await addAlertRuleButton.click();

    // Wait for form to appear
    await driver.wait(until.elementLocated(By.id("alert-form")), 5000);

    // Interact with form elements

    // Fill in the Rule Name
    let ruleNameInput = await driver.findElement(By.id("alert-rule-name"));
    await ruleNameInput.sendKeys("Test Alert Rule");

    // Select from dropdown (example with data source dropdown)
    let dataSourceDropdown = await driver.findElement(By.id("alert-data-source"));
    await dataSourceDropdown.click();
    let logsOption = await driver.findElement(By.id("option-1"));
    await logsOption.click();

    // Fill in the Query
    let queryInput = await driver.findElement(By.id("query"));
    await queryInput.sendKeys("city=Boston | stats count");

    // Select condition from dropdown
    let conditionDropdown = await driver.findElement(By.id("alert-condition"));
    await conditionDropdown.click();
    let conditionOption = await driver.findElement(By.id("option-0"));
    await conditionOption.click();

    // Fill in threshold value
    let thresholdInput = await driver.findElement(By.id("threshold-value"));
    await thresholdInput.sendKeys("100");

    // Fill in evaluation interval
    let evaluationInput = await driver.findElement(By.id("evaluate-every"));
    await evaluationInput.sendKeys("5");

    // Fill in notification message
    let messageInput = await driver.findElement(By.id("notification-msg"));
    await messageInput.sendKeys("Alert triggered!");

    // Click on Save button
    let saveButton = await driver.findElement(By.id("save-alert-btn"));
    await saveButton.click();
    
    await driver.get("http://localhost:5122/all-alerts.html");

    await driver.sleep(10000); // 5000 milliseconds = 5 seconds



    let newAlertRule = await driver.findElement(By.xpath("//span[contains(@class, 'ag-cell-value')]"))

    let isAlertRuleAdded = await newAlertRule.isDisplayed();
    assert.equal(isAlertRuleAdded, true, "New alert rule was not added to the table");

    console.log("All alerts page tests passed");
  } catch (err) {
    console.error(err);
  } finally {
    await driver.quit();
  }
}

testAllAlertsPage();
