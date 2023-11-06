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

let selectedTestData = "";

$(document).ready(function () {
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    $('.theme-btn').on('click', themePickerHandler);
    displayNavbar();
    let org_name;

    $.ajax({
        method: 'get',
        url: 'api/config',
        crossDomain: true,
        dataType: 'json',
        credentials: 'include'
    })
        .then((res) => {
            let token;
            myOrgSendTestData(token);
        })
        .catch((err) => {
            console.log(err)
        });

    $('#logout-btn').on('click', deleteCookie);
    $('.role-inner-dropdown').on('click', dropdown);

    $("ul").on("click", "li", function (e) {
        $(".select").html($(this).html());
        selectedTestData = $(this).attr('id');
        $('.dropdown-option').toggleClass('active');
    });
})

function dropdown() {
    $('.dropdown-option').toggleClass('active');
}

function sendTestData(e, token) {
    getIngestUrl()
        .then((ingestionUrl) => {
            if (token) {
                sendTestDataWithBearerToken(ingestionUrl, token).then((res) => {
                    showSendTestDataUpdateToast('Sent Test Data Successfully');
                    let testDataBtn = document.getElementById("test-data-btn");
                    testDataBtn.disabled = false;
                })
                    .catch((err) => {
                        console.log(err)
                        showSendTestDataUpdateToast('Error Sending Test Data');
                        let testDataBtn = document.getElementById("test-data-btn");
                        testDataBtn.disabled = false;
                    });
            } else {
                sendTestDataWithoutBearerToken(ingestionUrl).then((res) => {
                    showSendTestDataUpdateToast('Sent Test Data Successfully');
                    let testDataBtn = document.getElementById("test-data-btn");
                    testDataBtn.disabled = false;
                })
                    .catch((err) => {
                        console.log(err)
                        showSendTestDataUpdateToast('Error Sending Test Data');
                        let testDataBtn = document.getElementById("test-data-btn");
                        testDataBtn.disabled = false;
                    });
            }
        })
        .catch((err) => {
            console.error(err);
        });

    function sendTestDataWithBearerToken(ingestionUrl, token) {
        return new Promise((resolve, reject) => {
            $.ajax({
                method: 'post',
                url: ingestionUrl + '/api/sampledataset_bulk',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
                crossDomain: true,
                dataType: 'json',
                credentials: 'include'
            }).then((res) => {
                resolve(res);
            })
                .catch((err) => {
                    console.log(err);
                    reject(err);
                });
        });
    }

    function sendTestDataWithoutBearerToken(ingestionUrl) {
        return new Promise((resolve, reject) => {
            $.ajax({
                method: 'post',
                url: ingestionUrl + '/api/sampledataset_bulk',
                crossDomain: true,
                dataType: 'json',
                credentials: 'include'
            }).then((res) => {
                resolve(res);
            })
                .catch((err) => {
                    console.log(err);
                    reject(err);
                });
        });
    }
}
