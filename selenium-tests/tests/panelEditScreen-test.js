/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

const {By,Key,Builder, until, WebDriverWait} = require("selenium-webdriver");
const assert = require("assert");
const chrome = require('selenium-webdriver/chrome');
let driver;
const chromeOptions = new chrome.Options()

async function testEditPanelScreenOptions(){

    //To wait for browser to build and launch properly
    driver = await new Builder().forBrowser("chrome")
    .setChromeOptions(chromeOptions)
    .build();

    //To fetch http://localhost/dashboards-home.html from the browser with our code.
    await driver.get("http://localhost:5122/dashboards-home.html");

    let newDbBtn = await driver.findElement(By.id("create-db-btn"));
    await newDbBtn.click();
    // check if Dashboard is created
    let dbName = "ABC";
    let dbNameInput = await driver.findElement(By.id("db-name"));
    let dbDescInput = await driver.findElement(By.id("db-description"));
    let saveDbBtn = await driver.findElement(By.id("save-dbbtn"));

    await dbNameInput.sendKeys(dbName);
    await dbDescInput.sendKeys("This is a test description");
    await saveDbBtn.click();

    await driver.wait(until.urlContains('dashboard.html'), 5000);
    let currentUrl = await driver.getCurrentUrl();
    assert(currentUrl.includes("dashboard.html"), `URL ${currentUrl} does not contain "dashboard.html"`);
    await driver.sleep(1000);
    let addPanelBtn = await driver.findElement(By.id("add-panel-btn"));
    await addPanelBtn.click();

    let panelOptionsBtn = await driver.findElement(By.id("panel-options-btn"));
    await panelOptionsBtn.click();

    // Loop through the items and log their text
    let panelViewBtn = await driver.findElement(By.css(".panel-view-li"));
    const panelViewText = await panelViewBtn.getText();
    assert.equal(panelViewText, "View", 'button text is not "View"');
    await panelViewBtn.click();
    viewPanelModal = await driver.findElement(By.css(".panel"));
    let viewPanelModalPresent = await viewPanelModal.isDisplayed();
    assert.equal(viewPanelModalPresent, true, 'view-panel-modal is not displayed');
    panelOptionsBtn = await driver.findElement(By.id("panel-options-btn"));
    await panelOptionsBtn.click();
    let panelEditBtn = await driver.findElement(By.css(".panel-edit-li"));
    const panelEditText = await panelEditBtn.getText();
    assert.equal(panelEditText, "Edit", 'button text is not "Edit"');
    
    await panelEditBtn.click();
    let EditPanelScreenModal = await driver.findElement(By.css(".panelEditor-container"));
    let EditPanelScreenModalPresent = await EditPanelScreenModal.isDisplayed();
    assert.equal(EditPanelScreenModalPresent, true, 'EditPanelScreenModal is not displayed');

    // Check all buttons and options in edit panel screen
    let discardBtn = await driver.findElement(By.id("discard-btn"));
    let saveBtn = await driver.findElement(By.id("save-btn"));
    let applyBtn = await driver.findElement(By.id("apply-btn"));

    assert(await discardBtn.isDisplayed(), 'discardBtn is not displayed');
    assert(await saveBtn.isDisplayed(), 'saveBtn is not displayed');
    assert(await applyBtn.isDisplayed(), 'applyBtn is not displayed');

    assert(discardBtn.isEnabled(), 'discardBtn is not enabled');
    assert(saveBtn.isEnabled(), 'saveBtn is not enabled');
    assert(applyBtn.isEnabled(), 'applyBtn is not enabled');

    assert (await discardBtn.getText() == "Discard", 'discardBtn text is not "Discard"');
    assert (await saveBtn.getText() == "Save", 'saveBtn text is not "Save"');
    assert (await applyBtn.getText() == "Apply", 'applyBtn text is not "Apply"');

    // let timePickerBtn = await driver.findElement(By.id("date-picker-btn"));
    // assert.equal(timePickerBtn.getText(), "Time Picker", 'button text is not "Time Picker"');

    //test data source options
    let dataSourceBtn = await driver.findElement(By.css(".dropDownTitle"));
    await dataSourceBtn.click();
    await driver.sleep(1000);
    const dataSourcelist = await driver.findElement(By.css('ul.editPanelMenu-dataSource'));
    await driver.wait(until.elementIsVisible(dataSourcelist), 5000);
    let dataSourceOptions = await dataSourcelist.findElements(By.tagName('li'));
    let expectedDataSourceOptions = ["Metrics", "Logs", "Traces"];
    let actualDataSourceOptions = [];
    for (const item of dataSourceOptions) {
        const text = await item.getText();
        actualDataSourceOptions.push(text);
    }
    assert.deepEqual(actualDataSourceOptions, expectedDataSourceOptions, 'dataSourceOptions are not as expected');

    // test name and Description fields
    let nameInput = await driver.findElement(By.id("panEdit-nameChangeInput"));
    let descriptionInput = await driver.findElement(By.id("panEdit-descrChangeInput"));
    assert.equal(await nameInput.getAttribute("placeholder"), "Name", 'nameInput placeholder is not "Enter Panel Name"');
    assert.equal(await descriptionInput.getAttribute("placeholder"), "Description (Optional)", 'descriptionInput placeholder is not "Enter Panel Description"');

    // test panel type options
    let chartTypeBtn = await driver.findElement(By.css(".dropDown-chart"));
    chartTypeBtn.click();
    await driver.sleep(1000);
    const chartTypeList = await driver.findElement(By.css('ul.editPanelMenu-chart'));
    await driver.wait(until.elementIsVisible(chartTypeList), 5000);
    let chartTypeOptions = await chartTypeList.findElements(By.tagName('li'));

    let expectedPanelChartTypeOptions = ["Line", "Bar", "Pie", "Log Lines", "Number"];
    let actualPanelChartTypeOptions = [];
    for (const item of chartTypeOptions) {
        const text = await item.getText();
        actualPanelChartTypeOptions.push(text);
    }
    assert.deepEqual(actualPanelChartTypeOptions, expectedPanelChartTypeOptions, 'panelTypeOptions are not as expected');

    //todo test unit options(add when all the options are added)
    console.log("All Edit panel screen tests passed")
}

testEditPanelScreenOptions();