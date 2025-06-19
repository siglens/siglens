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
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

$(document).ready(function () {
    fetch('/api/license')
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            $('#licenseType').text(data.license_type || 'N/A');
            $('#licensedTo').text(data.licensed_to || 'N/A');
            $('#organization').text(data.organization || 'N/A');
            $('#version').text(data.version || 'N/A');
            $('#maxUsers').text(data.max_users || 'Unlimited');
            
            let expiryDate = 'N/A';
            if (data.expiry_date && data.expiry_date !== '') {
                try {
                    const date = new Date(data.expiry_date);
                    if (!isNaN(date.getTime())) {
                        expiryDate = date.toLocaleDateString();
                    }
                } catch (e) {
                    console.error('Error parsing date:', e);
                    expiryDate = data.expiry_date;
                }
            }
            $('#licenseExpiry').text(expiryDate);
        })
        .catch(error => {
            console.error('Error fetching license info:', error);
            $('#licenseType, #licensedTo, #organization, #version, #maxUsers, #licenseExpiry')
                .text('Error loading license data');
        });
        $('#theme-btn').on('click', function() {
        const html = $('html');
        const currentTheme = html.attr('data-theme');
        const newTheme = currentTheme === 'light' ? 'dark' : 'light';
        
        html.attr('data-theme', newTheme);
        Cookies.set('theme', newTheme, { expires: 365 });
    });
});