// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

    $.fn.appendOrgNavTabs = function (buttonArray) {
        var htmlBlock = `
            <div>
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
