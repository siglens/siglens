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

chromeOptions.addArguments('--headless');

async function testSavedQueriesPageButtons(){
    try{
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();
                                    
        await driver.get("http://localhost:5122/saved-queries.html");

        let searchButton = await driver.findElement(By.id("search-query-btn"));
        let btnTxt = await searchButton.getText();
        assert.equal(btnTxt, "Search", 'button text is not "Search"');

        let queriesGrid = await driver.findElement(By.id("queries-grid"));  
        let queriesGridPresent = await queriesGrid.isDisplayed();
        assert.equal(queriesGridPresent, true, 'queries-grid is not displayed');

        const initialAgGridRows = await driver.findElements(By.css('.ag-row'));
        assert(initialAgGridRows.length >=0, 'No rows found in the ag-Grid');

        if (initialAgGridRows.length > 0)
        {
            let delQueryBtn = await driver.findElement(By.id("delbutton"));
            await delQueryBtn.click();
            let deleteQueryModal = await driver.findElement(By.id("del-sq-popup"));
            let deleteQueryModalPresent = await deleteQueryModal.isDisplayed();
            assert.equal(deleteQueryModalPresent, true, 'delete-query-modal is not displayed');
            let deleteSQBtn = await driver.findElement(By.id("delete-btn"));
            let cancelSQBtn = await driver.findElement(By.id("cancel-btn"));

            await cancelSQBtn.click();
            await driver.sleep(1000);
            let currentAgGridRows = await driver.findElements(By.css('.ag-row'));
            assert.equal(initialAgGridRows.length ,currentAgGridRows.length, 'cancel button does not work')

            await delQueryBtn.click();

            await deleteSQBtn.click();
            await driver.sleep(1000);
            currentAgGridRows = await driver.findElements(By.css('.ag-row'));
            assert.notEqual(initialAgGridRows.length ,currentAgGridRows.length, 'delete button does not work');
        }
        console.log("All saved queries screen tests passed");

    }catch (err) {
        console.error(err);
    
    } finally {
        await driver.quit();
    }
}

testSavedQueriesPageButtons();