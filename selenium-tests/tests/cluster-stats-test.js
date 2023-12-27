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

async function testClusterStatsPageButtons(){
    try{
        //To wait for browser to build and launch properly
        driver = await new Builder().forBrowser("chrome")
                                    .setChromeOptions(chromeOptions)
                                    .build();

        await driver.get("http://localhost:5122/cluster-stats.html");  

         // test time picker button
        let timePickerBtn = await driver.findElement(By.id("date-picker-btn"));
        let datePickerTxt = await timePickerBtn.getText();
        assert.equal(datePickerTxt, "Time Picker", 'button text is not "Time Picker"');

        //test all charts
        const eventCountGraph = await driver.findElement(By.id('EventCountChart'));
        await driver.wait(until.elementIsVisible(eventCountGraph));

        const gbCountGraph = await driver.findElement(By.id('GBCountChart'));
        await driver.wait(until.elementIsVisible(gbCountGraph),5000);

        const totalVolumeGraph = await driver.findElement(By.id('TotalVolumeChart'));
        await driver.wait(until.elementIsVisible(totalVolumeGraph),5000);

        const storageSavingsPercent = await driver.wait(until.elementLocated(By.className('storage-savings-percent')), 5000);
        const isStorageSavingsDisplayed = await storageSavingsPercent.isDisplayed();
        assert.equal(isStorageSavingsDisplayed, true, 'Storage Savings Percent is not displayed');

        //test all tables
        const queryStatsContainer = await driver.findElement(By.className('query-stats'));
        const indexStatsContainer = await driver.findElement(By.className('index-stats'));
        await driver.sleep(1000);

        const isQueryStatsDisplayed = await queryStatsContainer.isDisplayed();
        assert.equal(isQueryStatsDisplayed, true, 'Query Stats container is not displayed');

        await driver.sleep(1000);
        const isIndexStatsDisplayed = await indexStatsContainer.isDisplayed();
        assert.equal(isIndexStatsDisplayed, true, 'Index Stats container is not displayed');

        const queryTable = await driver.findElement(By.id('query-table'));
        const isQueryTableExists = await queryTable.isDisplayed();
        assert.equal(isQueryTableExists, true, 'Query Table element exists');

        const indexDataTable = await driver.findElement(By.id('index-data-table'));
        const isIndexDataTableExists = await indexDataTable.isDisplayed();
        assert.equal(isIndexDataTableExists, true, 'Index Table element exists');

        console.log("All Cluster Stats Screen test passed")
    }
    catch (err) {
        // Handle any errors that occur during test execution
        console.error(err);
    
    } finally {
        // Close the driver instance, even if an error occurred
        await driver.quit();
    }
}

testClusterStatsPageButtons();