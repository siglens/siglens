// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.


const {By,Key,Builder, until, WebDriverWait} = require("selenium-webdriver");
const assert = require("assert");
const chrome = require('selenium-webdriver/chrome');
let driver;
const chromeOptions = new chrome.Options()

// chromeOptions.addArguments('--headless');

async function testDashboardPageButtons(){
    try{
        //To wait for browser to build and launch properly
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();
        
        //To fetch http://localhost/dashboards-home.html from the browser with our code.
        await driver.get("http://localhost:5122/dashboards-home.html");
        let searchButton = await driver.findElement(By.id("run-search"));
        let btnTxt = await searchButton.getText();
        assert.equal(btnTxt, "Search", 'button text is not "Search"');

        let newDbBtn = await driver.findElement(By.id("create-db-btn"));
        let newDbTxt = await newDbBtn.getText();
        assert.equal(newDbTxt, "New Dashboard", 'button text is not "Create New Database"');

        let dashboardGrid = await driver.findElement(By.id("dashboard-grid"));  
        // check if dashboard grid is displayed
        let dashboardGridPresent = await dashboardGrid.isDisplayed();
        assert.equal(dashboardGridPresent, true, 'dashboard-grid is not displayed');
    
        // check if the Add dashboard button is clickable
        await newDbBtn.click();
        let addDashboardModal = await driver.findElement(By.id("new-dashboard-modal"));
        let addDashboardModalPresent = await addDashboardModal.isDisplayed();
        assert.equal(addDashboardModalPresent, true, 'new-dashboard-modal is not displayed');

        // check if Dashboard is created
        let dbName = "Test Dashboard";
        let dbNameInput = await driver.findElement(By.id("db-name"));
        let dbDescInput = await driver.findElement(By.id("db-description"));
        let saveDbBtn = await driver.findElement(By.id("save-dbbtn"));
        // todo check if cancel button works
        let cancelDbBtn = await driver.findElement(By.id("cancel-dbbtn"));
        const initialUrl = 'http://localhost:5122/dashboards-home.html';

        await cancelDbBtn.click();
        await driver.sleep(1000);
        let currentUrl = await driver.getCurrentUrl();
        assert.equal(currentUrl, initialUrl, 'cancel button does not work');

        await newDbBtn.click();
        await dbNameInput.sendKeys(dbName);
        await dbDescInput.sendKeys("Test Description");
        await saveDbBtn.click();

        await driver.wait(until.urlContains('dashboard.html'), 5000);
        currentUrl = await driver.getCurrentUrl();
        assert(currentUrl.includes("dashboard.html"), `URL ${currentUrl} does not contain "dashboard.html"`);

        await driver.sleep(1000);

        // Now check buttons on launch-dashboard.html
       // test add panel button
        let addPanelBtn = await driver.findElement(By.id("add-panel-btn"));
        let addPanelTxt = await addPanelBtn.getText();
        assert.equal(addPanelTxt, "Add Panel", 'button text is not "Add Panel"');

        // test save db button
        let saveDBBtn = await driver.findElement(By.id("save-db-btn"));
        const saveDbImage = await saveDBBtn.findElement(By.css("img"));
        let saveIconClassAttr = await saveDbImage.getAttribute("src");
        assert(saveIconClassAttr.endsWith('/save-icon.svg'), 'Image icon does not point to expected file');

        // test settings button
        let settingsBtn = await driver.findElement(By.className("settings-btn"));
        const settingsImage = await settingsBtn.findElement(By.css("img"));
        let settingsIconClassAttr = await settingsImage.getAttribute("src");
        assert(settingsIconClassAttr.endsWith('/settings-icon.svg'), 'Image icon does not point to expected file');

        // test refresh button
        let refreshBtn = await driver.findElement(By.className("refresh-btn"));
        const refreshImage = await refreshBtn.findElement(By.css("img"));
        let refreshIconClassAttr = await refreshImage.getAttribute("src");
        assert(refreshIconClassAttr.endsWith('/refresh-icon.svg'), 'Image icon does not point to expected file');

        // test time picker button
        let timePickerBtn = await driver.findElement(By.id("date-picker-btn"));
        let datePickerTxt = await timePickerBtn.getText();
        assert.equal(datePickerTxt, "Time Picker", 'button text is not "Time Picker"');

        //todo check if add panel btn works
        addPanelBtn = await driver.findElement(By.id("add-panel-btn"));
        await addPanelBtn.click();
        
        let panel = driver.findElement(By.className("panel"))
        
        let panelPresent = await panel.isDisplayed();
        assert.equal(panelPresent, true, 'panel is not displayed');

        // check panel options
        let panelOptionsBtn = await driver.findElement(By.id("panel-options-btn"));
        await panelOptionsBtn.click();
        let panelOptionsModal = await driver.findElement(By.id("panel-dropdown-modal"));
        let panelOptionsModalPresent = await panelOptionsModal.isDisplayed();
        assert.equal(panelOptionsModalPresent, true, 'panel-dropdown-modal is not displayed');


        // Find the dropdown list elements
        const list = await driver.findElement(By.css('ul.dropdown-style'));

        // Find all the child elements of the list
        const items = await list.findElements(By.tagName('li'));

        // Loop through the items and log their text
        let expectedOptionList = ["View", "Edit", "Duplicate", "Remove"];
        let actualOptionList = [];
        for (const item of items) {
            const text = await item.getText();
            actualOptionList.push(text);
        }
        assert.deepEqual(actualOptionList, expectedOptionList, 'panel options are not as expected');

        // click save db button
        await saveDBBtn.click();

        let saveDbModal = await driver.findElement(By.id("save-db-modal"));
        let saveDbModalPresent = await saveDbModal.isDisplayed();
        assert.equal(saveDbModalPresent, true, 'save-db-modal is not displayed');
        let saveDbModalTxt = await saveDbModal.getText();
        assert.equal(saveDbModalTxt, "Dashboard Updated Successfully ✖", 'modal text is not "Dashboard Updated Successfully "');
        
        // check if the dashboard is saved
        let allDashboardsText = await driver.findElement(By.id("all-dashboards-text"));
    
        await allDashboardsText.click();

        // check if you are redirected to launch-dashboard.html
        await driver.wait(until.urlContains('dashboards-home.html'), 5000);
        const currentUrl2 = await driver.getCurrentUrl();
    
        assert.equal(currentUrl2, initialUrl, 'not redirected to dashboards-home.html');

        // check if the dashboard is saved
        const aggridRows = await driver.findElements(By.css('.ag-row'));
        assert(aggridRows.length > 1, 'No rows found in the ag-Grid');

        console.log("All dashboard tests passed")
    }catch (err) {
        // Handle any errors that occur during test execution
        console.error(err);
    
    } finally {
        // Close the driver instance, even if an error occurred
        await driver.quit();
    }
    
}

testDashboardPageButtons();