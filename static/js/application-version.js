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

$(document).ready(function () {

    $('.theme-btn').on('click', themePickerHandler);
    fetchVersionInfo();
    {{ .Button1Function }}
});

function fetchVersionInfo() {

    $.ajax({
        method: "GET",
        url: "/api/version/info",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        const versionInfo = 'SigLens Version: ' + res.version;
        $('#versionInfo').text(versionInfo);
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.error('Error fetching version:', textStatus, errorThrown);
        $('#versionInfo').text('Error loading version');
    });
}


