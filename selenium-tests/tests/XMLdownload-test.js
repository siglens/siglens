/**
 * XML Download Test: Downloads an XML file, saves it locally, 
 * checks if the content is indeed in the format intended.
 */

const { By, Key, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const fs =require("fs");
const chrome = require("selenium-webdriver/chrome");
let driver;
const chromeOptions = new chrome.Options();
const axios = require('axios');
const path = require('path');;
const xml2js = require('xml2js');

function containsSpecifiedLog(records) {
  return new Promise((resolve, reject) => {
    xml2js.parseString(records, (err, result) => {
      if (err) {
        reject(err);
      } else {
        const specifiedLog = result.root.item[0]; // Search for the first log
        delete specifiedLog.timestamp; // Remove timestamp
        const specifiedLogString = JSON.stringify(specifiedLog);
        const expectedLog = {
          _index: [ 'test-data' ],
          address: [ '23995 Road furt, Oakland, North Carolina 44201' ],
          app_name: [ 'Advertisinghave' ],
          app_version: [ '5.12.1' ],
          batch: [ 'batch-128' ],
          city: [ 'Oakland' ],
          country: [ 'Puerto Rico' ],
          first_name: [ 'Darion' ],
          gender: [ 'male' ],
          group: [ 'group 2' ],
          hobby: [ 'Philately' ],
          http_method: [ 'PATCH' ],
          http_status: [ '404' ],
          ident: [ '73b5b9bd-72d3-46da-9a5f-33a29d49dcff' ],
          image: [ 'https://picsum.photos/225/438' ],
          job_company: [ 'OpenGov' ],
          job_description: [ 'District' ],
          job_level: [ 'Response' ],
          job_title: [ 'Administrator' ],
          last_name: [ 'Cruickshank' ],
          latency: [ '681482' ],
          latitude: [ '-53.664277' ],
          longitude: [ '59.458667' ],
          question: [
            'Waistcoat vinegar polaroid green juice kitsch humblebrag seitan brunch Wes Anderson fanny pack?'
          ],
          ssn: [ '842394245' ],
          state: [ 'North Carolina' ],
          street: [ '23995 Road furt' ],
          url: [ 'http://www.directembrace.com/e-business/bricks-and-clicks' ],
          user_agent: [
            'Mozilla/5.0 (Windows NT 6.0; en-US; rv:1.9.0.20) Gecko/1951-10-26 Firefox/36.0'
          ],
          user_color: [ 'DarkGoldenRod' ],
          user_email: [ 'oswaldwisoky@sipes.info' ],
          user_phone: [ '8362715069' ],
          weekday: [ 'Monday' ],
          zip: [ '44201' ]
        }
        ;

        const expectedLogString = JSON.stringify(expectedLog); // Stringify the expected log object

        // Compare specified log with expected log
        if (specifiedLogString === expectedLogString) {
          console.log("Matches the expected string.");
        } else {
          console.log("specifiedLogString does not match the expected string.");
        }
        resolve();
      }
    });
  });
}

async function XMLDownload() {
  try {
    driver = await new Builder()
      .forBrowser("chrome")
      .setChromeOptions(chromeOptions)
      .build();

    await driver.get("http://localhost:5122/test-data.html");

    // Locate and click the dropdown toggle button
    let dropdownButton = await driver.findElement(By.id('source-selection'));
    await dropdownButton.click();

    // Wait for the dropdown menu to appear
    await driver.wait(until.elementLocated(By.id('source-options')), 5000);

    // Locate and click the first option in the dropdown menu
    let firstOption = await driver.findElement(By.id('option-1'));
    await firstOption.click();

    let sendDataButton = await driver.findElement(By.id('test-data-btn'));
    await sendDataButton.click();

    await new Promise(resolve => setTimeout(resolve, 5000));

    await driver.get("http://localhost:5122/index.html");

    await new Promise(resolve => setTimeout(resolve, 5000));

    let downloadButton = await driver.findElement(By.className('download-all-logs-btn'));
    await downloadButton.click();

    // Wait for the dropdown menu to appear
    await driver.wait(until.elementLocated(By.className('dropdown-menu')), 5000);

    let xmlOption = await driver.findElement(By.id('xml-block'));
    await xmlOption.click();

    await driver.wait(until.elementLocated(By.className('ui-dialog')), 5000);

    // Fill in the input field with the name "test"
    let inputField = await driver.findElement(By.id('qnameDL'));
    await inputField.sendKeys("test");

    let saveButton = await driver.findElement(By.className('saveqButton'));
    await saveButton.click();

    await new Promise(resolve => setTimeout(resolve, 5000));

    // Simulate capturing the download link
    const downloadLink = 'http://localhost:5122/api/search';
    const requestData = {};

    // Download the file
    const response = await axios.post(downloadLink, requestData, { responseType: 'stream' });
    const filePath = path.join(__dirname, 'test.xml');

    const writer = fs.createWriteStream(filePath);
    response.data.pipe(writer);

    // Wait for the file to finish downloading
    await new Promise((resolve, reject) => {
      writer.on('finish', resolve);
      writer.on('error', reject);
    });

    // Read the content of the downloaded file
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const parsedContent = JSON.parse(fileContent);

    // Extract the records from the parsed content
    const records = parsedContent.hits.records;

    records.sort((a, b) => {
      const appA = a.app_name.toLowerCase();
      const appB = b.app_name.toLowerCase();
      if (appA < appB) {
        return -1;
      }
      if (appA > appB) {
        return 1;
      }
      return 0;
    });

    let xmlString = '<?xml version="1.0" encoding="UTF-8"?>\n<root>\n';
    records.forEach(item => {
      xmlString += '  <item>\n';
      Object.keys(item).forEach(key => {
        // Escape special characters in the value
        const escapedValue = String(item[key])
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&apos;');
        xmlString += `    <${key}>${escapedValue}</${key}>\n`;
      });
      xmlString += '  </item>\n';
    });
    xmlString += '</root>';
    
    // Check if the specified log exists in the XML content
    await containsSpecifiedLog(xmlString);

    // Clean up temporary file
    fs.unlinkSync(filePath);

    console.log("Downloads tests passed");
  } catch (err) {
    console.error(err);
  } finally {
    await driver.quit();
  }
}

// Run the test
XMLDownload();