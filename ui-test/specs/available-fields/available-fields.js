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

/**
 * siglens
 * (c) 2022 - All rights reserved.
 */

'use strict';

describe('avaialble fields',  () => {
    let page;

    before(async () => {
        page = await browser.newPage();

        page.on('error', (err) => {
            console.error('error happen at the page: ', err);
        });

        page.on('pageerror', (pageerr) => {
            console.error('pageerror occurred: ', pageerr);
        });

        await page.goto(`file://${__dirname}/available-fields.html`);
    });

    afterEach(async () => {
        // reset after each test
        await page.evaluate(() => {
            defaultColumnCount = 0;
            resetAvailableFields();
        });
    });

    it('should have the correct page title', async () => {
        expect(await page.title()).to.eql('SigLens - Available Fields');
    });

    it.skip('should have the first 3 fields checked', async () => {
        // const selector = '#available-fields .fields';
        let selector = '#available-fields .toggle-field';

        await page.evaluate(() => {
            defaultColumnCount = 3;
            let columnOrder = [
                "_index",
                "batch",
                "browser_language",
                "browser_name",
                "browser_user_agent",
                "click_id",
                "device_is_mobile",
                "device_type",
                "eventType",
                "event_id",
                "geo_country",
                "geo_latitude",
                "geo_longitude",
                "geo_region_name",
                "geo_timezone",
                "ip_address",
                "os",
                "os_name",
                "os_timezone",
                "page_url",
                "page_url_path",
                "referer_medium",
                "referer_url",
                "referer_url_port",
                "referer_url_scheme",
                "timestamp",
                "user_custom_id",
                "user_domain_id",
                "utm_campaign",
                "utm_content",
                "utm_medium",
                "utm_source"
            ];
            renderAvailableFields(columnOrder);
        });

        await page.waitForSelector(selector);

        let fields = await page.$$eval(selector, (elements) => {
            // this is running in the webpage's context
            // so we'll need to use a plain object to pass back data
            return elements.map(el => {
                return {
                    html: el.outerHTML,
                    index: el.dataset.index,
                    checked: $(el).find('.toggle-field').hasClass('active')
                };
            });
        });

        console.log('>>>', fields);

        // only the first 3 should be checked
        expect(fields[0].checked).to.be.true;
        expect(fields[1].checked).to.be.true;
        expect(fields[2].checked).to.be.true;

        expect(fields[3].checked).to.be.false;
    });


    it.skip('should have the first 2 fields checked', async () => {
        let selector = '#available-fields .toggle-field';

        await page.evaluate(() => {
            defaultColumnCount = 2;
            let columnOrder = [
                "_index",
                "batch",
                "browser_language",
                "browser_name",
                "browser_user_agent",
                "click_id",
                "device_is_mobile",
                "device_type",
                "eventType",
                "event_id",
                "geo_country",
                "geo_latitude",
                "geo_longitude",
                "geo_region_name",
                "geo_timezone",
                "ip_address",
                "os",
                "os_name",
                "os_timezone",
                "page_url",
                "page_url_path",
                "referer_medium",
                "referer_url",
                "referer_url_port",
                "referer_url_scheme",
                "timestamp",
                "user_custom_id",
                "user_domain_id",
                "utm_campaign",
                "utm_content",
                "utm_medium",
                "utm_source"
            ];
            renderAvailableFields(columnOrder);
        });

        await page.waitForSelector(selector);

        let fields = await page.$$eval(selector, (elements) => {
            // this is running in the webpage's context
            // so we'll need to use a plain object to pass back data
            return elements.map(el => {
                return {
                    html: el.outerHTML,
                    index: el.dataset.index,
                    checked: $(el).find('.toggle-field').hasClass('fa-square-check')
                };
            });
        });

        // only the first 2 should be checked
        expect(fields[0].checked).to.be.true;
        expect(fields[1].checked).to.be.true;

        expect(fields[2].checked).to.be.false;
        expect(fields[3].checked).to.be.false;
    });

    it('should reset the available field', async () => {
        await page.click('#reset-fields');

        let fields = await page.$eval('#available-fields .fields', (el) => {
            return el.innerHTML;
        });

        expect(fields).to.be.empty;
    });
});