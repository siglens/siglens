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

(function ($) {
    $.fn.orgNavTabs = function () {
        return this.each(function () {
            var $container = $(this);
            var $tabs = $container.find('.section-button');

            $container.on('click', '.section-button', function (e) {
                e.preventDefault();
                $tabs.removeClass('active');
                $(this).addClass('active');
                var href = $(this).find('a').attr('href');
                window.location.href = href;
            });

            // Add active class based on the current URL
            var currentUrl = window.location.href;
            var currentTab = extractLastPathSegment(currentUrl);
            $tabs.removeClass('active');
            if (currentTab === 'all-alerts' || currentTab === 'alert' || currentTab === 'alert-details') {
                $tabs.filter('[id="all-alerts"]').addClass('active');
            } else {
                $tabs.filter(`[id="${currentTab}"]`).addClass('active');
            }
        
        });
    };

    function extractLastPathSegment(url) {
        return (new URL(url).pathname.match(/[^/]+\/?$/)[0] || '').replace(/\..+$/, '');
    }

    $.fn.appendOrgNavTabs = function (header,buttonArray) {
        var htmlBlock = `
            <div>
                <h1 class="myOrg-heading">${header}</h1>
                <div class="section-buttons">
        `;

        buttonArray.forEach(function (button) {
            htmlBlock += `
                <div class="section-button" id="${button.class}"><a href="${button.url}">${button.name}</a></div>
            `;
        });

        htmlBlock += `
                </div>
            </div>
        `;

        this.prepend(htmlBlock);
        this.orgNavTabs();
    };
})(jQuery);
