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

describe('search indices', () => {
    let page;

    before(async () => {
        page = await browser.newPage();

        page.on('error', (err) => {
            console.error('error happen at the page: ', err);
        });

        page.on('pageerror', (pageerr) => {
            console.error('pageerror occurred: ', pageerr);
        });

        // page.on('request', (req) => {
        //     console.log('request', req.url);
        // });

        // page.on('requestfailed', (err) => {
        //     console.log('request failed', err);
        // });

        await page.goto(`file://${__dirname}/index.html`);
        // await page.waitForNavigation();

        page.on('console', (msg) => {
            console.log('PAGE LOG:', msg.text());
        });
    });

    it('should have the correct page title', async () => {
        expect(await page.title()).to.eql('SigLens - Index');
    });

    it('should render the index dropdown', async () => {

        //evaluate in page context
        await page.evaluate(() => {
            let data = [
                {"index": "foo"},
                {"index": "bar"}
            ];
            renderIndexDropdown(data);
        });

        let indexOptions = await page.$$eval('.index-dropdown-item', (elements) => {
            // this is running in the webpage's context
            // so we'll need to use a plain object to pass back data
            return elements.map(el => {
                return {
                    html: el.outerHTML,
                    // index: el.dataset.index,
                    // checked: $(el).find('.toggle-field').hasClass('fa-square-check')
                };
            });
        });

        //console.log(indexOptions);
        expect(indexOptions).to.have.length(3);

    });

});