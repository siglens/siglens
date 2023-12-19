# Addding Selenium Tests

## Overview
This repository contains a suite of Selenium tests designed to validate the functionality of a web application. Selenium is a powerful tool for automating browser interactions, and these tests are intended to ensure the robustness and correctness of the application under different scenarios.

## Prerequisites
Before running the Selenium tests, make sure you have the following prerequisites installed on your machine:

* **Node.js**: Ensure you have Node.js installed, as it is required for running JavaScript-based Selenium tests.

* **npm**: npm is the package manager for JavaScript. It is included with Node.js. Make sure you have a recent version installed. You can update npm using the following command:

```sh
npm install -g npm@latest
```

* WebDriver: WebDriver is a component of Selenium that provides a programming interface for controlling web browsers. Install the necessary WebDriver for your browser.

For Chrome:
```sh
npm install --save selenium-webdriver chromedriver
```

For Firefox:
```sh
npm install --save selenium-webdriver geckodriver
```

* Running the Tests
1. Clone the Repository
Clone this repository to your local machine:

```sh
git clone <repository-url>
cd <repository-directory>
```

2. Install Dependencies
Navigate to the selenium-tests directory and install the project dependencies:

```sh
cd selenium-tests
npm install
```

3. Run Individual Test
To run an individual test, execute the following command, replacing <test-file> with the desired test file:

```sh
node tests/<test-file>
```
For example:
```sh
node tests/panelEditScreen-test.js
```

4. Run All Tests
To run all the tests, use the following command:

```sh
npm run test
```
This command will execute all the test files present in the tests directory.

## Documentation

* https://www.selenium.dev/selenium/docs/api/javascript/index.html



## Writing Selenium Tests
If you are interested in writing additional Selenium tests or modifying existing ones, consider the following:

### Understanding Selenium WebDriver:
Familiarize yourself with Selenium WebDriver and its documentation: Selenium WebDriver Documentation

### Reviewing Existing Test Files:
Examine the structure and content of the existing test files in the tests directory to understand how tests are written using the Selenium WebDriver API.

### Using Assertions:
Leverage assertions from the testing framework (e.g., assert module in Node.js) to validate expected outcomes against actual results.

### Organizing Test Logic:
Structure your test logic into functions and use try/catch blocks for error handling.

### Interacting with Page Elements:
Use the findElement method to locate and interact with page elements (buttons, input fields, etc.). Familiarize yourself with various locators like By.id, By.className, etc.

Feedback and Contributions
Feel free to provide feedback, report issues, or contribute to this test suite by submitting pull requests. We welcome contributions that improve the coverage and effectiveness of the tests.

Happy testing! 🚀