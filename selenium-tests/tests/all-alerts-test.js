const { By, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const chrome = require('selenium-webdriver/chrome');

let driver;
const chromeOptions = new chrome.Options();
chromeOptions.addArguments('--headless');

async function testAlertsPage() {
    try {
        // Launch the browser
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();

        // Navigate to the alerts page
        await driver.get("http://localhost/all-alerts.html");

        // Wait for the "Add Alert Rule" button to be visible and clickable
        let addAlertRuleButton = await driver.findElement(By.id("new-alert-rule"));
        let addAlertButtonText = await addAlertRuleButton.getText();
        assert.equal(addAlertButtonText.includes("Add Alert Rule"), true, '"Add Alert Rule" button text is not correct');
    

        // Check if the alert grid container is displayed
        let isAlertGridContainerDisplayed = await driver.findElement(By.id("alert-grid-container")).isDisplayed();
        assert.equal(isAlertGridContainerDisplayed, true, "Alert grid container is not displayed");

        console.log("All Alerts Page tests passed");
    } catch (err) {
        console.error(err);
    } finally {
        // Close the browser
        await driver.quit();
    }
}

// Run the tests
testAlertsPage();
