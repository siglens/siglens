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

$(document).ready(function () {
    if (Cookies.get('theme')) {
        var theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    $('.theme-btn').on('click', themePickerHandler);
    displayNavbar();

    function showPersistentQueryWarning(value) {
        if (value === 'disabled') {
            alert("Disabling persistent queries may affect your query performance.");
        }
    }

    function updatePersistentQueriesSetting(pqsConfig) {
        $.ajax({
            method: "POST",
            url: "/api/pqs/update",
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                Accept: "*/*",
            },
            dataType: "json",
            crossDomain: true,
            data: JSON.stringify({ pqsConfig: pqsConfig }),
            success: function (res) {
                console.log("Update successful:", res);
            },
            error: function (xhr, status, error) {
                console.error("Update failed:", xhr, status, error);
            },
        });
    }

    $('#persistent-queries-dropdown').on('change', function() {
        showPersistentQueryWarning(this.value);
        updatePersistentQueriesSetting(this.value);
    });
});


