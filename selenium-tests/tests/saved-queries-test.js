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