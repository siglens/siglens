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


const {By,Key,Builder} = require("selenium-webdriver");
const assert = require("assert");
const chrome = require('selenium-webdriver/chrome');
let driver;
const chromeOptions = new chrome.Options()

chromeOptions.addArguments('--headless');

async function testIndexPageButtons(){
    try{
        //To wait for browser to build and launch properly
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();
        
        //To fetch http://localhost/index.html from the browser with our code.
        
        await driver.get("http://localhost:5122/index.html");
            
        // To find search buttons on the index.html page
        
        let indexButton = await driver.findElement(By.id("index-btn"));
        let indexTxt = await indexButton.getText();
        let datePickerBtn = await driver.findElement(By.id("date-picker-btn"));
        let datePickerTxt = await datePickerBtn.getText();
        // let saveQueryBtn = await driver.findElement(By.id("saveq-btn"));
        // let saveQueryTxt = await saveQueryBtn.getText();
        // let availableFieldBtn = await driver.findElement(By.id("avail-fields-btn"));
        // let availableFieldTxt = await availableFieldBtn.getText();
        let searchButton = await driver.findElement(By.id("run-filter-btn"));
        let btnTxt = await searchButton.getText();
        
        assert.equal(btnTxt, "Search", 'button text is not "Search"');
        assert.equal(indexTxt, "Index", 'button text is not "Index"');
        assert.equal(datePickerTxt, "Last 15 Mins", 'button text is not "Date Picker"');
        // assert.equal(saveQueryTxt, "Save Query", 'button text is not "Save Query"');
        // assert.equal(availableFieldTxt, "  Available Fields", 'button text is not "Available Fields"');

        // check if the dropdowns are displayed
        let dropdownPresent;
        await indexButton.click();
        dropdownPresent = await driver.findElement(By.id("available-indexes")).isDisplayed();
        assert.equal(dropdownPresent, true, 'available-indexes is not displayed');
        await indexButton.click();

        await datePickerBtn.click();
        dropdownPresent = await driver.findElement(By.id("daterangepicker ")).isDisplayed();
        assert.equal(dropdownPresent, true, 'daterangepicker is not displayed');
        await datePickerBtn.click();

        // await availableFieldBtn.click();
        // dropdownPresent = await driver.findElement(By.id("available-fields")).isDisplayed();
        // assert.equal(dropdownPresent, true, 'available-fields is not displayed');
        // await availableFieldBtn.click();
        console.log("All buttons tests passed")
    }catch (err) {
        // Handle any errors that occur during test execution
        console.error(err);
    
    } finally {
        // Close the driver instance, even if an error occurred
        // await driver.quit();
    }
}

testIndexPageButtons();

async function testSearchInvalidQuery(){
    try{
        //To wait for browser to build and launch properly
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();
        
        //To fetch http://localhost/index.html from the browser with our code.
        await driver.get("http://localhost:5122/index.html");
            
        // Enter text in the search box
        let searchBox = await driver.findElement(By.id("filter-input"));

        // get the placeholder text using the getAttribute method
        let placeholderText = await searchBox.getAttribute('placeholder');

        // type the search query using the sendKeys method
        // test invalid query
        await searchBox.sendKeys('derf(1234');
        await driver.sleep(500);
        // submit the search by pressing Enter
        await searchBox.sendKeys(Key.RETURN);
        let cornerText = await driver.findElement(By.id("corner-text")).getText();
        let closeBtn = await driver.findElement(By.id("close-btn"));
        assert.equal(cornerText, 'Message: 1:5 (4): no match found, expected: "!=", ".", "<", "<=", "=", ">", ">=", "|", [ \\n\\t\\r], [-a-zA-Z0-9$&,?#%_@;[\\]{}+-./*:]i or EOF', "Invalid Query");

        closeBtn.click();
        await driver.sleep(5000);
        // test query with no results
        await searchBox.clear();
        await searchBox.sendKeys('test');
        await searchBox.sendKeys(Key.RETURN);

        let emptyResponse = await driver.findElement(By.id("empty-response"));
        let emptyResponseText = await emptyResponse.getText();

        assert.equal(emptyResponseText, 'Your query returned no data, adjust your query.', "No results found for your query");
        await driver.sleep(10000);
        console.log("All search query test passed")

    }catch (err) {
        // Handle any errors that occur during test execution
        console.error(err);
    
    } finally {
        // Close the driver instance, even if an error occurred
        await driver.quit();
    }
}

testSearchInvalidQuery();