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