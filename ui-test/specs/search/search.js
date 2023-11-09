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

describe('search', () => {
    let page;

    before(async () => {
        page = await browser.newPage();

        page.on('error', (err) => {
            console.error('error happen at the page: ', err);
        });

        page.on('pageerror', (pageerr) => {
            console.error('pageerror occurred: ', pageerr);
        });

        page.on('requestfailed', (err) => {
            console.log(err);
        });

        await page.goto(`file://${__dirname}/search.html`);
        // await page.waitForNavigation();

        page.on('conosle', (msg) => {
            console.log('PAGE LOG:', msg.text());
        });
    });

    it('should have the correct page title', async () => {
        expect(await page.title()).to.eql('SigLens - Search');
    });

});