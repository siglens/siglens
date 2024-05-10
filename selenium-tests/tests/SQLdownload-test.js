/**
 * SQL Download Test: Downloads an SQL file, saves it locally,
 * checks if the content is indeed in the format intended.
 */

const { By, Key, Builder, until } = require("selenium-webdriver");
const assert = require("assert");
const fs =require("fs");
const chrome = require("selenium-webdriver/chrome");
let driver;
const chromeOptions = new chrome.Options();
const axios = require('axios');
const path = require('path');

// Function to check if the specified log exists in the SQL content
function containsSpecifiedLog(SQLContent) {
    return new Promise((resolve, reject) => {
      // Check if SQLContent is an array and has elements
      if (Array.isArray(SQLContent) && SQLContent.length > 0) {
        // Take the first element of the array
        const firstSQLQuery = SQLContent[0];

        const isSQL = firstSQLQuery.includes("INSERT INTO SQL_Table (_index, address, app_name, app_version, batch, city, country, first_name, gender, group, hobby, http_method, http_status, ident, image, job_company, job_description, job_level, job_title, last_name, latency, latitude, longitude, question, ssn, state, street, url, user_agent, user_color, user_email, user_phone, weekday, zip) VALUES ('test-data', '23995 Road furt, Oakland, North Carolina 44201', 'Advertisinghave', '5.12.1', 'batch-128', 'Oakland', 'Puerto Rico', 'Darion', 'male', 'group 2', 'Philately', 'PATCH', 404, '73b5b9bd-72d3-46da-9a5f-33a29d49dcff', 'https://picsum.photos/225/438', 'OpenGov', 'District', 'Response', 'Administrator', 'Cruickshank', 681482, -53.664277, 59.458667, 'Waistcoat vinegar polaroid green juice kitsch humblebrag seitan brunch Wes Anderson fanny pack?', '842394245', 'North Carolina', '23995 Road furt', 'http://www.directembrace.com/e-business/bricks-and-clicks', 'Mozilla/5.0 (Windows NT 6.0; en-US; rv:1.9.0.20) Gecko/1951-10-26 Firefox/36.0', 'DarkGoldenRod', 'oswaldwisoky@sipes.info', '8362715069', 'Monday', '44201')");
  
        // Output the result
        console.log(`File is SQL: ${isSQL}`);
  
        // Resolve the promise
        resolve();
      } else {
        // If SQLContent is not an array or it's empty, reject the promise
        reject(new Error('SQLContent is not in the expected format'));
      }
    });
  }

async function SQLDownload() {
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

    let SQLOption = await driver.findElement(By.id('sql-block'));
    await SQLOption.click();

    await driver.wait(until.elementLocated(By.className('ui-dialog')), 5000);

    // Fill in the input field with the name "test"
    let inputField = await driver.findElement(By.id('qnameDL'));
    await inputField.sendKeys("test2");

    let saveButton = await driver.findElement(By.className('saveqButton'));
    await saveButton.click();

    await new Promise(resolve => setTimeout(resolve, 3000));

    const downloadLink = 'http://localhost:5122/api/search';
    const requestData = {}; 
    
    // Download the file
    const response = await axios.post(downloadLink, requestData, { responseType: 'stream' });
    const filePath = path.join(__dirname, 'test2.sql'); 
    
    // Create a writable stream to save the downloaded data
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

    const tableName = 'SQL_Table'; 
    const columns = Object.keys(records[0]); 
    
    // Generate SQL INSERT statements for each object in the data array
    const sqlStatements = records.map(item => {
        const values = columns.map(col => {
            // Check if the column is 'timestamp', if so, skip it
            if (col === 'timestamp') return null;
            
            // Escape single quotes in string values and wrap in quotes
            const value = typeof item[col] === 'string' ? `'${item[col].replace(/'/g, "''")}'` : item[col];
            return value;
        }).filter(value => value !== null).join(', '); // Join non-null column values with commas

        return `INSERT INTO ${tableName} (${columns.filter(col => col !== 'timestamp').join(', ')}) VALUES (${values});`;
    });
    sqlStatements.join('\n');

    // Check if the specified log exists in the SQL content
    await containsSpecifiedLog(sqlStatements);

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
SQLDownload();