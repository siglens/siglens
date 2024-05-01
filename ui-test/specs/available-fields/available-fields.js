/* 
 * Copyright (c) 2021-2024 SigScalr, Inc.
 *
 * This file is part of SigLens Observability Solution
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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