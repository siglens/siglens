const { By, Key, Builder, WebElement } = require("selenium-webdriver");
const assert = require("assert");
const chrome = require('selenium-webdriver/chrome');
let driver;
const chromeOptions = new chrome.Options()
chromeOptions.addArguments('--headless');

async function checkIncrementor(Component){
    const initialValue = await Component.getAttribute('value');
    // Attempt to increment the value
    await Component.sendKeys(Key.ARROW_UP);
    // Get the value after incrementing
    const incrementedValue = await Component.getAttribute('value');
    // Assert that the value has been incremented
    assert.notStrictEqual(incrementedValue, initialValue, 'Failed to increment the number input.');
    // Attempt to decrement the value
    await Component.sendKeys(Key.ARROW_DOWN);
    // Get the value after decrementing
    const decrementedValue = await Component.getAttribute('value');
    assert.notStrictEqual(decrementedValue, incrementedValue, 'Failed to decrement the number input.');

}
async function testAlertPagesButtons() {
    try{
        driver = await new Builder().forBrowser("chrome")
            .setChromeOptions(chromeOptions)
            .build();
    
        //To fetch http://localhost/alert.html from the browser with our code.
    
        await driver.get("http://localhost:5122/alert.html");
        let cancelBtn = driver.findElement(By.id("cancel-alert-btn"));
        let cancelTxt = await cancelBtn.getText();
        let saveBtn = driver.findElement(By.id("save-alert-btn"));
        let saveTxt = await saveBtn.getText();
    
        // Checks for cancel and save button existance 
        assert.equal(cancelTxt, "Cancel", 'button text is not "Cancel"');
        assert.equal(saveTxt, "Save", 'button text is not "Save"');
    
        // checks if Alert Rule Text is editable
        let alertRuleInputTextBox = await driver.findElement(By.id("alert-rule-name"));
        const checkAlertRuleEditable = await alertRuleInputTextBox.isEnabled();
        assert.strictEqual(checkAlertRuleEditable, true, 'alert rule input text box is not editable');
    
        let logsBtn = await driver.findElement(By.id("alert-data-source"));
        await logsBtn.click();
        let logsDrpDownPresent= await driver.findElement(By.id("data-source-options")).isDisplayed();
        assert.strictEqual(logsDrpDownPresent, true, 'logs dropdown is not displayed');
        await logsBtn.click();

        let logsLanguageBtn = await driver.findElement(By.id('logs-language-btn'));
        await logsLanguageBtn.click();
        let logsLanguageDrpDownPresent= await driver.findElement(By.id("logs-language-options")).isDisplayed();
        assert.strictEqual(logsLanguageDrpDownPresent, true, 'logs language dropdown is not displayed');
        await logsLanguageBtn.click();
        
        let datePickerBtn = await driver.findElement(By.id('date-picker-btn'));
        await datePickerBtn.click();
        let datePickerDrpDownPresent= await driver.findElement(By.id("daterangepicker ")).isDisplayed();
        assert.strictEqual(datePickerDrpDownPresent, true, 'date picker dropdown is not displayed');
        await datePickerBtn.click();

        let queryTextBox = await driver.findElement(By.id('query'));
        const checkQueryEditable = await queryTextBox.isEnabled();
        assert.strictEqual(checkQueryEditable, true, 'query text box is not editable');

        let alertThresholdTextBox = await driver.findElement(By.id('alert-condition'));
        await alertThresholdTextBox.click();
        let alertThresholdDrpDownPresent= await driver.findElement(By.className("dropdown-menu box-shadow alert-condition-options show")).isDisplayed();
        assert.strictEqual(alertThresholdDrpDownPresent, true, 'alert threshold dropdown is not displayed');
        await alertThresholdTextBox.click();

        let alertThresholdInputTextBox = await driver.findElement(By.id('threshold-value'));
        const checkAlertThresholdEditable = await alertThresholdInputTextBox.isEnabled();
        assert.strictEqual(checkAlertThresholdEditable, true, 'alert threshold input text box is not editable');
        await checkIncrementor(alertThresholdInputTextBox);

        let evaluateEveryTextBox = await driver.findElement(By.id('evaluate-every'));
        const checkEvaluateEveryEditable = await evaluateEveryTextBox.isEnabled();
        assert.strictEqual(checkEvaluateEveryEditable, true, 'evaluate every input text box is not editable');
        await checkIncrementor(evaluateEveryTextBox);

        let evaluteForTextBox = await driver.findElement(By.id('evaluate-for'));

        const checkAlertForEditable = await evaluteForTextBox.isEnabled();
        assert.strictEqual(checkAlertForEditable, true, 'alert for input text box is not editable');
        await checkIncrementor(evaluteForTextBox);


        let contactsPtsBtn = await driver.findElement(By.id('contact-points-dropdown'));
        await contactsPtsBtn.click();
        let contactPtsDrpDownPresent= await driver.findElement(By.className("dropdown-menu box-shadow contact-points-options show")).isDisplayed();
        assert.strictEqual(contactPtsDrpDownPresent, true, 'contact points dropdown is not displayed');
        let  contactPtsDrpDown=await driver.findElement(By.className("dropdown-menu box-shadow contact-points-options show"))
        let addNewBtnForContact = await contactPtsDrpDown.findElement(By.id('option-0'));
        assert.strictEqual(await addNewBtnForContact.getText(), 'Add New', 'Add new button is not present');
        await addNewBtnForContact.click();

        let cancelBtnForContact = await driver.findElement(By.id('cancel-contact-btn'));
        let cancelBtnTxt = await cancelBtnForContact.getText();
        assert.strictEqual(cancelBtnTxt, 'Cancel', 'Cancel button is not present');

        let saveBtnForContact = await driver.findElement(By.id('save-contact-btn'));
        let saveBtnTxt = await saveBtnForContact.getText();

        assert.strictEqual(saveBtnTxt, 'Save', 'Save button is not present');
        let contactNameInputTextBox = await driver.findElement(By.id('contact-name'));

        const checkContactNameEditable = await contactNameInputTextBox.isEnabled();
        assert.strictEqual(checkContactNameEditable, true, 'contact name input text box is not editable');

        let contactTypeDrpDown = await driver.findElement(By.id('contact-types'));
        await contactTypeDrpDown.click();
        let contactTypeDrpDownPresent= await driver.findElement(By.className("dropdown-menu box-shadow contact-options show"));

        assert.strictEqual(await contactTypeDrpDownPresent.isDisplayed(), true, 'contact type dropdown is not displayed');

        let slackOptionElement = await contactTypeDrpDownPresent.findElement(By.id('option-0'));
        await slackOptionElement.click();
        let slackChannelInputTextBox = await driver.findElement(By.id('slack-channel-id'));
        const checkSlackChannelEditable = await slackChannelInputTextBox.isEnabled();
        assert.strictEqual(checkSlackChannelEditable, true, 'slack channel input text box is not editable');
        let slackTokenInputTextBox = await driver.findElement(By.id('slack-token'));
        const checkSlackTokenEditable = await slackTokenInputTextBox.isEnabled();
        assert.strictEqual(checkSlackTokenEditable, true, 'slack token input text box is not editable');

        contactTypeDrpDown.click();

        let webHookOptionElement = await contactTypeDrpDownPresent.findElement(By.id('option-1'));
        await webHookOptionElement.click();
        let webHookInputTextBox = await driver.findElement(By.id('webhook-id'));
        const checkWebHookEditable = await webHookInputTextBox.isEnabled();
        assert.strictEqual(checkWebHookEditable, true, 'webhook input text box is not editable');

        let addNewButton = await driver.findElement(By.className('add-new-contact-type btn'));
        assert.strictEqual(await addNewButton.getText(), 'Add new contact type', 'Add new button is not present');

        cancelBtnForContact.click();

        let notificationMessageInputTextBox = await driver.findElement(By.id('notification-msg'));
        const checkMessageEditable = await notificationMessageInputTextBox.isEnabled();
        assert.strictEqual(checkMessageEditable, true, 'notification message input text box is not editable');

        let labelKeyInputTextBox = await driver.findElement(By.id('label-key'));
        const checkLabelKeyEditable = await labelKeyInputTextBox.isEnabled();
        assert.strictEqual(checkLabelKeyEditable, true, 'label key input text box is not editable');
        labelKeyInputTextBox.getAttribute('value').then(function (value) {
            assert.strictEqual(value, 'alerting', 'label key input text box is not editable');
        });

        let labelValueInputTextBox = await driver.findElement(By.id('label-value'));
        const checkLabelValueEditable = await labelValueInputTextBox.isEnabled();
        assert.strictEqual(checkLabelValueEditable, true, 'label value input text box is not editable');
        labelValueInputTextBox.getAttribute('value').then(function (value) {
            assert.strictEqual(value, 'true', 'label value input text box is not editable');
        });

        let addNewLabelButton = await driver.findElement(By.className('add-label-container btn'));
        assert.strictEqual(await addNewLabelButton.getText(), 'Add Label', 'Add new label button is not present');

    }
    catch (err) {
        // Handle any errors that occur during test execution
        console.error(err);
    }
    finally {
        // Close the browser
        await driver.quit();

    }

}

testAlertPagesButtons();
