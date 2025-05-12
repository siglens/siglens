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

let contactGridDiv = null;
let contactRowData = [];
var contactData = {};
var allContactsArray;
let contactEditFlag = 0;

/* Contact Form Component - This component is used on both the "contacts.html" and "alert.html" pages. */
const contactFormHTML = `
<form id="contact-form">
<div class="d-flex btn-container">
    <button class="btn btn-secondary mx-3" id="cancel-contact-btn" type="button">Cancel</button>
    <button class="btn btn-primary" id="save-contact-btn" type="submit">Save</button>
</div>
<div class="add-contact-form">
    <div>
        <label for="contact-name">Contact point name</label>
        <input type="text" class="form-control" placeholder="Enter a contact point name" id="contact-name" required >
    </div>
    <div id="main-container">
        <div class="contact-container">
        <div class="mb-0 d-flex justify-content-between">
            <div class="mb-0">
                <label for="type">Type</label>
                <div class="dropdown">
                    <button class="btn dropdown-toggle" type="button" id="contact-types"
                        data-toggle="dropdown" aria-haspopup="true" aria-expanded="false"
                        data-bs-toggle="dropdown">
                        <span>Slack</span>
                        <i class="dropdown-arrow"></i>
                    </button>
                    <div class="dropdown-menu box-shadow contact-options">
                        <li id="option-0" class="contact-option active">Slack</li>
                        <li id="option-1" class="contact-option">Webhook</li>
                    </div>
                </div>
            </div>
            <div class="button-container mb-0">
                <button class="btn d-flex align-items-center justify-content-center test-contact-btn" type="button">
                    <div class="send-icon"></div>
                    <div class="mb-0">Test</div>
                </button>
            </div>
        </div>
            <div class="slack-container">
    <div>
    <div style="position: relative;">
        <label for="slack-channel-id">Channel ID</label>
        <input type="text" class="form-control" id="slack-channel-id" style="position: relative;">
        <i class="fa fa-info-circle position-absolute info-icon" rel="tooltip" style="display: block; top: 23px;"
        title="Specify channel, private group, or IM channel (can be an encoded ID or a name)."
        id="info-slack-channel-id"></i>
    </div>
    </div>
    <div style="position: relative;">
    <label for="slack-token">Slack API Token</label>
    <input type="text" class="form-control" id="slack-token" style="position: relative;">
        <i class="fa fa-info-circle position-absolute info-icon" rel="tooltip" style="display: block; top: 23px;"
        title="Provide a Slack bot API token (starts with “xoxb”)."
        id="info-slack-token"></i>
    </div>
</div>
<div class="webhook-container">
    <label for="webhook">Webhook URL</label>
    <input type="text" class="form-control" id="webhook-id">
    <div class="headers-section mt-3">
        <label for="additional-headers">Custom Headers</label>
        <p>Optionally provide extra custom headers to be used in the request.</p>
        
        <div class="headers-main-container">
        <div class="headers-labels">
            <div class="header-label">Header Name</div>
            <div class="header-label">Header Value</div>
        </div>
            <!-- Added headers will appear here -->
        </div>
        <button class="add-headers-container btn btn-grey" type="button">
            <span class="plus-icon">+</span>Add Header
        </button>
    </div>
</div>
    </div>
    <button class="add-new-contact-type btn btn-primary" type="button">
        <span>
            <img src="./assets/add-icon.svg" class="add-icon">Add new contact type
        </span>
    </button>
</div>
</form>
`;

$(document).ready(function () {
    $('.theme-btn').on('click', themePickerHandler);
    $('#new-contact-point').on('click', initializeContactForm);
    $('#contact-form-container').css('display', 'none');

    getAllContactPoints();
    if (window.location.href.includes('alert.html')) {
        initializeContactForm();
    }

    const tooltipIds = ['info-slack-channel-id', 'info-slack-token'];

    tooltipIds.forEach((id) => {
        $(`#${id}`)
            .tooltip({
                delay: { show: 0, hide: 300 },
                trigger: 'click',
            })
            .on('click', function () {
                $(`#${id}`).tooltip('show');
            });
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest('.tooltip-inner').length === 0) {
            tooltipIds.forEach((id) => $(`#${id}`).tooltip('hide'));
            $('.button-container').tooltip('hide');
        }
    });
});

$(document).on('click', '.contact-option', setContactTypes);

function updateDeleteButtonVisibility() {
    const contactContainers = $('.contact-container');
    if (contactContainers.length > 1) {
        contactContainers.find('.del-contact-type').show();
    } else {
        contactContainers.find('.del-contact-type').hide();
    }
}

function initializeContactForm(contactId) {
    $('#new-contact-point').css('display', 'none');
    $('#alert-grid-container').css('display', 'none');
    $('#contact-form-container').css('display', 'block');

    initializeBreadcrumbs([
        { name: 'Alerting', url: './alerting.html' },
        { name: 'Contact Points', url: './contacts.html' },
        { name: 'New Contact Point', url: '#' },
    ]);
    
    const formContainer = $('#contact-form-container');

    if (formContainer) {
        formContainer.html(contactFormHTML); // Use .html() to replace the content to avoid appending multiple times.
        $('.slack-container').css('display', 'block');
        $('.webhook-container').css('display', 'none');
        $('.headers-labels').css('display', 'none'); 
        if (contactEditFlag) {
            showContactFormForEdit(contactId);
        } else {
            $('.contact-container').first().find('.button-container').append('<button class="btn-simple del-contact-type" type="button"></button>');
        }
    }

    const contactForm = $('#contact-form');
    contactForm.on('submit', (e) => submitAddContactPointForm(e));

    $('#cancel-contact-btn').on('click', function () {
        resetContactForm();
        if (window.location.href.includes('alert.html')) {
            $('.popupOverlay, .popupContent').removeClass('active');
            $('#main-container .contact-container:gt(0)').remove();
        } else {
            window.location.href = '../contacts.html';
        }
    });

    updateDeleteButtonVisibility();

    $('.add-new-contact-type').on('click', function () {
        addNewContactTypeContainer();
        updateTestButtonState();
    });

    $('#main-container').on('click', '.del-contact-type', function () {
        $(this).closest('.contact-container').remove();
        updateDeleteButtonVisibility();
    });

    $('#main-container').on('click', '.test-contact-btn', function () {
        const container = $(this).closest('.contact-container');
        getContactPointTestData(container);
    });

    $('#main-container').on('click', '.add-headers-container', function () {
        const headersMainContainer = $(this).closest('.headers-section').find('.headers-main-container');
        const hasIncompleteHeaders = headersMainContainer.find('.headers-container').filter(function () {
            const key = $(this).find('#header-key').val();
            const value = $(this).find('#header-value').val();
            return (!key || !value) && $(this).find('.tick-icon').length > 0;
        }).length > 0;
    
        if (hasIncompleteHeaders) {
            alert('Please complete or cancel the current header before adding a new one.');
            return;
        }
    
        const newHeadersContainer = `
            <div class="headers-container">
                <input type="text" id="header-key" class="form-control" placeholder="Header name" tabindex="7" value="">
                <span class="headers-gap"></span>
                <input type="text" id="header-value" class="form-control" placeholder="Header Value" tabindex="8" value="">
                <button class="tick-icon" type="button" id="confirm-header"></button>
                <button class="cross-icon" type="button" id="cancel-header"></button>
            </div>
        `;
        headersMainContainer.append(newHeadersContainer);
        updateAddHeaderButtonState($(this));
    });

    $('#main-container').on('click', '.headers-main-container .tick-icon', function () {
        const headerContainer = $(this).closest('.headers-container');
        const key = headerContainer.find('#header-key').val();
        const value = headerContainer.find('#header-value').val();
        const headersMainContainer = headerContainer.closest('.headers-main-container');
        const headersSection = headersMainContainer.closest('.headers-section');
    
        if (key && value) {
            const displayHeader = `
                <div class="headers-container">
                    <input type="text" id="header-key" class="form-control" placeholder="Header name" tabindex="7" value="${key}" readonly>
                    <span class="headers-gap"></span>
                    <input type="text" id="header-value" class="form-control" placeholder="Header Value" tabindex="8" value="${value}" readonly>
                    <button class="edit-icon" type="button" id="edit-header"></button>
                    <button class="delete-icon" type="button" id="delete-header"></button>
                </div>
            `;
            headersMainContainer.append(displayHeader);
            headerContainer.remove();
            headersSection.find('.headers-labels').css('display', 'flex'); 
            updateAddHeaderButtonState(headersSection.find('.add-headers-container'));
        } else {
            alert('Please fill in both header name and value.');
        }
    });
    
    $('#main-container').on('click', '.headers-main-container .cross-icon', function () {
        const headerContainer = $(this).closest('.headers-container');
        const headersMainContainer = headerContainer.closest('.headers-main-container');
        const headersSection = headersMainContainer.closest('.headers-section');
        headerContainer.remove();
        if (headersMainContainer.children().length === 0) {
            headersSection.find('.headers-labels').css('display', 'none');
        }
        updateAddHeaderButtonState(headersSection.find('.add-headers-container'));
    });
    
    $('#main-container').on('click', '.headers-main-container .edit-icon', function () {
        const headerContainer = $(this).closest('.headers-container');
        const key = headerContainer.find('#header-key').val();
        const value = headerContainer.find('#header-value').val();
        const headersMainContainer = headerContainer.closest('.headers-main-container');
        const headersSection = headersMainContainer.closest('.headers-section');
    
        const editHeaderContainer = `
            <div class="headers-container">
                <input type="text" id="header-key" class="form-control" placeholder="Header name" tabindex="7" value="${key}">
                <span class="headers-gap"></span>
                <input type="text" id="header-value" class="form-control" placeholder="Header Value" value="${value}" tabindex="8">
                <button class="tick-icon" type="button" id="confirm-header"></button>
                <button class="cross-icon" type="button" id="cancel-header"></button>
            </div>
        `;
        headerContainer.replaceWith(editHeaderContainer);
        headersSection.find('.headers-labels').css('display', 'flex');
        updateAddHeaderButtonState(headersSection.find('.add-headers-container'));
    });
    
    $('#main-container').on('click', '.headers-main-container .delete-icon', function () {
        const headerContainer = $(this).closest('.headers-container');
        const headersMainContainer = headerContainer.closest('.headers-main-container');
        const headersSection = headersMainContainer.closest('.headers-section');
        headerContainer.remove();
        if (headersMainContainer.children().length === 0) {
            headersSection.find('.headers-labels').css('display', 'none');
        }
        updateAddHeaderButtonState(headersSection.find('.add-headers-container'));
    });

    function updateAddHeaderButtonState($addButton) {
        const headersMainContainer = $addButton.closest('.headers-section').find('.headers-main-container');
        const hasIncompleteHeaders = headersMainContainer.find('.headers-container').filter(function () {
            const key = $(this).find('#header-key').val();
            const value = $(this).find('#header-value').val();
            return (!key || !value) && $(this).find('.tick-icon').length > 0;
        }).length > 0;

        if (hasIncompleteHeaders) {
            $addButton.addClass('disabled').prop('disabled', true);
        } else {
            $addButton.removeClass('disabled').prop('disabled', false);
        }
    }
    
    updateTestButtonState();
    $('#contact-form').on('input', function () {
        updateTestButtonState();
    });
}

function hasUnconfirmedHeaders() {
    let hasUnconfirmed = false;
    $('.headers-main-container').each(function () {
        const unconfirmedHeaders = $(this).find('.headers-container').filter(function () {
            const key = $(this).find('#header-key').val();
            const value = $(this).find('#header-value').val();
            return $(this).find('.tick-icon').length > 0 && (key || value);
        });
        if (unconfirmedHeaders.length > 0) {
            hasUnconfirmed = true;
            return false; 
        }
    });
    return hasUnconfirmed;
}

function setContactForm() {
    contactData.contact_name = $('#contact-name').val();
    contactData.slack = [];
    contactData.webhook = [];

    $('.contact-container').each(function () {
        let contactType = $(this).find('#contact-types span').text();

        if (contactType === 'Slack') {
            let slackValue = $(this).find('#slack-channel-id').val();
            let slackToken = $(this).find('#slack-token').val();
            if (slackValue && slackToken) {
                let slackContact = {
                    channel_id: slackValue,
                    slack_token: slackToken,
                };
                contactData.slack.push(slackContact);
            }
        } else if (contactType === 'Webhook') {
            let webhookValue = $(this).find('#webhook-id').val();
            if (webhookValue) {
                let headers = {};
                $(this).find('.headers-container').each(function () {
                    const keyInput = $(this).find('#header-key');
                    const valueInput = $(this).find('#header-value');
                    // Only include headers that are confirmed (i.e., readonly inputs with edit/delete icons)
                    if (
                        keyInput.length &&
                        valueInput.length &&
                        keyInput.val() &&
                        valueInput.val() &&
                        keyInput.prop('readonly') &&
                        $(this).find('.edit-icon').length > 0
                    ) {
                        headers[keyInput.val()] = valueInput.val();
                    }
                });

                let webhookContact = {
                    webhook: webhookValue,
                    headers: headers,
                };
                contactData.webhook.push(webhookContact);
            }
        }
    });
    contactData.pager_duty = '';
    updateTestButtonState();
}

function updateTestButtonState() {
    $('.contact-container').each(function () {
        const isSlack = $(this).find('#contact-types span').text() === 'Slack';
        const channelId = $(this).find('#slack-channel-id').val();
        const slackToken = $(this).find('#slack-token').val();
        const webhookUrl = $(this).find('#webhook-id').val();

        const isFormValid = isSlack ? channelId && slackToken : webhookUrl;

        const $testButton = $(this).find('.test-contact-btn');

        if (isFormValid) {
            $testButton.removeClass('disabled');
            if ($testButton[0]._tippy) {
                $testButton[0]._tippy.destroy();
            }
        } else {
            $testButton.addClass('disabled');
            if (!$testButton[0]._tippy) {
                //eslint-disable-next-line no-undef
                tippy($testButton[0], {
                    content: 'Please fill all required fields.',
                    delay: [0, 300],
                });
            }
        }

        $testButton.tooltip('dispose'); // Remove existing tooltip if any
    });
}

function addNewContactTypeContainer() {
    if (hasUnconfirmedHeaders()) {
        alert('Please confirm or cancel all headers before adding a new contact type.');
        return;
    }

    let newContactContainer = $('.contact-container').first().clone();
    newContactContainer.find('.form-control').val('');
    newContactContainer.find('.headers-main-container').empty();
    newContactContainer.find('.headers-labels').css('display', 'none');
    newContactContainer.find('.del-contact-type').remove(); // Remove any existing delete buttons
    newContactContainer.find('.button-container').append('<button class="btn-simple del-contact-type" type="button"></button>');
    newContactContainer.appendTo('#main-container');

    const newChannelIdInfoId = 'info-slack-channel-id-' + Date.now();
    const newTokenInfoId = 'info-slack-token-' + Date.now();

    newContactContainer.find('.fa-info-circle').eq(0).attr('id', newChannelIdInfoId);
    newContactContainer.find('.fa-info-circle').eq(1).attr('id', newTokenInfoId);

    [newChannelIdInfoId, newTokenInfoId].forEach((id) => {
        $(`#${id}`).tooltip({
            delay: { show: 0, hide: 300 },
            trigger: 'hover',
        });
    });

    $('.add-new-contact-type').appendTo('#main-container'); // Move the button to the end
    updateDeleteButtonVisibility();
    updateTestButtonState();
}

function setContactTypes() {
    const selectedOption = $(this).html();
    const container = $(this).closest('.contact-container');

    const headersMainContainer = container.find('.headers-main-container');
    const hasUnconfirmedHeaders = headersMainContainer.find('.headers-container').filter(function () {
        const key = $(this).find('#header-key').val();
        const value = $(this).find('#header-value').val();
        return $(this).find('.tick-icon').length > 0 && (key || value);
    }).length > 0;

    if (hasUnconfirmedHeaders) {
        alert('Please confirm or cancel all headers before changing the contact type.');
        return;
    }

    container.find('.contact-option').removeClass('active');
    container.find('#contact-types span').html(selectedOption);
    $(this).addClass('active');
    updateTestButtonState();
    // Remove invalid class from all inputs
    container.find('.slack-container input, .webhook-container input').removeClass('is-invalid').val('');

    // Hide all contact type containers
    container.find('.slack-container, .webhook-container').css('display', 'none');

    container.find('.headers-main-container').empty();
    container.find('.headers-labels').css('display', 'none');

    // Show and set invalid class based on selected option
    if (selectedOption === 'Slack') {
        container.find('.slack-container').css('display', 'block');
    } else if (selectedOption === 'Webhook') {
        container.find('.webhook-container').css('display', 'block');
    }
}

function resetContactForm() {
    $('#contact-form input').val('');
    $('#contact-types span').text('Slack');
    $('.slack-container').css('display', 'block');
    $('.webhook-container').css('display', 'none');
    $('.headers-main-container').empty(); 
    $('.headers-labels').css('display', 'none');
    $('.contact-option').removeClass('active');
    $('.contact-options #option-0').addClass('active');
    contactData = {};
    updateTestButtonState();
}

function submitAddContactPointForm(e) {
    e.preventDefault();

    if (hasUnconfirmedHeaders()) {
        alert('Please confirm or cancel all headers before saving.');
        return;
    }

    if (!validateContactForm()) {
        alert('Please fill out all required fields.');
        return;
    }

    setContactForm();
    contactEditFlag ? updateContactPoint(contactData) : createContactPoint(contactData);
}

function createContactPoint() {
    $.ajax({
        method: 'post',
        url: 'api/alerts/createContact',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(contactData),
        dataType: 'json',
        crossDomain: true,
    })
        .then((res) => {
            if (window.location.href.includes('alert.html')) {
                $('.popupOverlay, .popupContent').removeClass('active');
                getAllContactPoints(contactData.contact_name);
            } else {
                window.location.href = '../contacts.html';
            }
            resetContactForm();
            showToast(res.message, 'success');
        })
        .catch((err) => {
            if (window.location.href.includes('alert.html')) {
                $('.popupOverlay, .popupContent').removeClass('active');
            }
            showToast(err.responseJSON.error, 'error');
        });
}

function updateContactPoint() {
    $.ajax({
        method: 'post',
        url: 'api/alerts/updateContact',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(contactData),
        dataType: 'json',
        crossDomain: true,
    })
        .then((res) => {
            resetContactForm();
            window.location.href = '../contacts.html';
            showToast(res.message, 'success');
        })
        .catch((err) => {
            showToast(err.responseJSON.error, 'error');
        });
}

function getAllContactPoints(contactName) {
    $.ajax({
        method: 'get',
        url: 'api/alerts/allContacts',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        allContactsArray = res.contacts;
        if (window.location.href.includes('alert.html')) {
            if (contactName) {
                const contact = allContactsArray.find((contact) => contact.contact_name === contactName);
                $('#contact-points-dropdown span').html(contact.contact_name);
                $('#contact-points-dropdown span').attr('id', contact.contact_id);
            }
        } else displayAllContacts(res.contacts);
    });
}

function deleteContactPrompt(data) {
    $('#contact-name-placeholder').html('<strong>' + data.contactName + '</strong>');
    $('.popupOverlay, .popupContent').addClass('active');
    $('#cancel-btn, .popupOverlay').click(function () {
        $('.popupOverlay, .popupContent').removeClass('active');
    });
    $('#delete-btn').off('click');
    $('#delete-btn').on('click', function () {
        $.ajax({
            method: 'delete',
            url: 'api/alerts/deleteContact',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            data: JSON.stringify({
                contact_id: data.contactId,
            }),
            crossDomain: true,
        })
            .then(function (res) {
                let deletedRowID = data.rowId;
                contactGridOptions.api.applyTransaction({
                    remove: [{ rowId: deletedRowID }],
                });

                showToast(res.message, 'success');
                $('.popupOverlay, .popupContent').removeClass('active');
            })
            .catch((err) => {
                showToast(err.responseJSON.error, 'error');
            });
    });
}

class btnRenderer {
    init(params) {
        this.eGui = document.createElement('span');
        this.eGui.innerHTML = `<div id="alert-grid-btn">
                <button class='btn' id="editbutton"></button>
                <button class="btn-simple mx-4" id="delbutton"></button>
                </div>`;
        this.eButton = this.eGui.querySelector('.btn');
        this.dButton = this.eGui.querySelector('.btn-simple');

        this.eButton.addEventListener('click', function () {
            contactEditFlag = 1;
            initializeContactForm(params.data.contactId);
        });
        this.dButton.addEventListener('click', () => getAllAlertsWithSameContactPoint(params.data));
    }

    getGui() {
        return this.eGui;
    }
    refresh(_params) {
        return false;
    }
}

function showDeleteContactDialog(data, matchingAlertNames) {
    $('#contact-name-placeholder-delete-dialog').html('<strong>' + data.contactName + '</strong>');
    $('.popupOverlay, .delete-dialog').addClass('active');
    let el = $('#associated-alerts');
    el.html(``);
    const maxHeight = 100;
    matchingAlertNames.forEach(function (alertName) {
        el.append(`<li class="alert-dropdown-item">${alertName}</li>`);
    });

    // Apply styling to make the dropdown scrollable
    el.css({
        'max-height': `${maxHeight}px`,
        'overflow-y': 'auto',
    });
    $('body').css('cursor', 'default');
    $('#cancel-btn, .popupOverlay').click(function () {
        $('.popupOverlay, .delete-dialog').removeClass('active');
    });
}

function getAllAlertsWithSameContactPoint(data) {
    const contactId = data.contactId;
    const matchingAlertNames = [];
    $.ajax({
        method: 'get',
        url: 'api/allalerts',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        if (res.alerts) {
            for (const alert of res.alerts) {
                if (alert.contact_id === contactId) {
                    matchingAlertNames.push(alert.alert_name);
                }
            }
        }
        if (matchingAlertNames.length > 0) {
            showDeleteContactDialog(data, matchingAlertNames);
        } else {
            deleteContactPrompt(data);
        }
    });
}

let contactColumnDefs = [
    {
        field: 'rowId',
        hide: true,
    },
    {
        field: 'contactId',
        hide: true,
    },
    {
        headerName: 'Contact point name',
        field: 'contactName',
        sortable: true,
        width: 100,
    },
    {
        headerName: 'Type',
        field: 'type',
        width: 100,
    },
    {
        headerName: 'Actions',
        cellRenderer: btnRenderer,
        width: 50,
    },
];

const contactGridOptions = {
    columnDefs: contactColumnDefs,
    rowData: contactRowData,
    animateRows: true,
    rowHeight: 34,
    headerHeight: 26,
    defaultColDef: {
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        cellClass: 'align-center-grid',
        resizable: true,
        sortable: true,
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    getRowId: (params) => params.data.rowId,
    onGridReady(params) {
        this.gridApi = params.api;
    },
};

function capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function validateContactForm() {
    let isValid = true;

    $('.contact-container').each(function () {
        const contactType = $(this).find('#contact-types span').text();
        if (contactType === 'Slack') {
            const slackValue = $(this).find('#slack-channel-id').val();
            const slackToken = $(this).find('#slack-token').val();
            if (!slackValue || !slackToken) {
                isValid = false;
                $(this)
                    .find('#slack-channel-id, #slack-token')
                    .each(function () {
                        if (!$(this).val()) {
                            $(this).addClass('is-invalid');
                        } else {
                            $(this).removeClass('is-invalid');
                        }
                    });
            }
        } else if (contactType === 'Webhook') {
            const webhookValue = $(this).find('#webhook-id').val();
            if (!webhookValue) {
                isValid = false;
                $(this).find('#webhook-id').addClass('is-invalid');
            } else {
                $(this).find('#webhook-id').removeClass('is-invalid');
            }
        }
    });

    return isValid;
}

function displayAllContacts(res) {
    if (contactGridDiv === null) {
        contactGridDiv = document.querySelector('.all-contacts-grid');
        //eslint-disable-next-line no-undef
        new agGrid.Grid(contactGridDiv, contactGridOptions);
    }
    contactGridOptions.api.setColumnDefs(contactColumnDefs);
    let newRow = new Map();
    $.each(res, function (key, value) {
        let contactType = Object.entries(value)
            .filter(([k, v]) => v != null && v.length !== 0 && k !== 'contact_name' && k !== 'contact_id' && k !== 'org_id')
            .map(([k, v]) => {
                return v.length > 1 ? `${capitalize(k)} (${v.length})` : capitalize(k);
            });
        let type = contactType.join(', ');

        newRow.set('rowId', key);
        newRow.set('contactId', value.contact_id);
        newRow.set('contactName', value.contact_name);
        newRow.set('type', type);
        contactRowData = _.concat(contactRowData, Object.fromEntries(newRow));
    });
    contactGridOptions.api.setRowData(contactRowData);
    contactGridOptions.api.sizeColumnsToFit();
}

//Edit Contact Point
function showContactFormForEdit(contactId) {
    let data = allContactsArray.find(function (obj) {
        return obj.contact_id === contactId;
    });
    $('#contact-name').val(data.contact_name);
    initializeBreadcrumbs([
        { name: 'Alerting', url: './alerting.html' },
        { name: 'Contact Points', url: './contacts.html' },
        { name: data.contact_name? data.contact_name : 'New Contact Point', url: '#' },
    ]);
    let isFirst = true;
    let containerCount = 0;

    Object.keys(data).forEach(function (key) {
        if (key === 'contact_name' || key === 'contact_id') {
            return;
        }

        let value = data[key];
        if (value != null && value.length > 0) {
            value.forEach(function (value) {
                let contactContainer;
                if (isFirst) {
                    contactContainer = $('.contact-container').first(); // Select the first container directly
                    isFirst = false;
                } else {
                    contactContainer = $('.contact-container').first().clone().removeClass('d-none'); // Clone the first container for subsequent ones
                    contactContainer.find('.button-container .del-contact-type').remove(); // Remove existing delete buttons from the cloned container
                    contactContainer.find('.headers-main-container').empty(); 
                    contactContainer.find('.headers-labels').css('display', 'none');
                    contactContainer.appendTo('#main-container');
                }

                contactContainer.find('#contact-types span').text(key.charAt(0).toUpperCase() + key.slice(1));
                if (key === 'slack') {
                    contactContainer.find('.webhook-container').css('display', 'none');
                    contactContainer.find('.slack-container').css('display', 'block');
                    contactContainer.find('.slack-container #slack-channel-id').val(value.channel_id);
                    contactContainer.find('.slack-container #slack-token').val(value.slack_token);
                }
                if (key === 'webhook') {
                    contactContainer.find('.webhook-container').css('display', 'block');
                    contactContainer.find('.slack-container').css('display', 'none');
                    contactContainer.find('.webhook-container #webhook-id').val(value.webhook);
                    if (value.headers && Object.keys(value.headers).length > 0) {
                        Object.entries(value.headers).forEach(([headerKey, headerValue]) => {
                            const displayHeader = `
                                <div class="headers-container">
                                    <input type="text" id="header-key" class="form-control" placeholder="Header name" tabindex="7" value="${headerKey}" readonly>
                                    <span class="headers-gap"></span>
                                        <input type="text" id="header-value" class="form-control" placeholder="Header Value" tabindex="8" value="${headerValue}" readonly>
                                    <button class="edit-icon" type="button" id="edit-header"></button>
                                    <button class="delete-icon" type="button" id="delete-header"></button>
                                </div>
                            `;
                            contactContainer.find('.headers-main-container').append(displayHeader);
                        });
                        contactContainer.find('.headers-labels').css('display', 'flex'); 
                    }
                }
                if (key != 'slack' && key != 'webhook') {
                    contactContainer.find(`.${key}-container .form-control`).val(value);
                }

                containerCount++;
            });
        }
    });

    // Add delete button to all containers if there are multiple containers
    if (containerCount > 1) {
        $('.contact-container').each(function () {
            if (!$(this).find('.del-contact-type').length) {
                $(this).find('.button-container').append('<button class="btn-simple del-contact-type" type="button"></button>');
            }
        });
    } else {
        // Ensure delete button is removed if only one container
        $('.contact-container').find('.del-contact-type').remove();
    }

    $('.add-new-contact-type').appendTo('#main-container'); // Move the button to the end

    updateDeleteButtonVisibility(); // Ensure delete buttons are updated after form initialization

    if (contactEditFlag) {
        contactData.contact_id = data.contact_id;
    }
}

function getContactPointTestData(container) {
    if (validateContactForm()) {
        let contactData = {};
        let contactType = container.find('#contact-types span').text();

        if (contactType === 'Slack') {
            let slackValue = container.find('#slack-channel-id').val();
            let slackToken = container.find('#slack-token').val();
            if (slackValue && slackToken) {
                contactData = {
                    type: 'slack',
                    settings: {
                        channel_id: slackValue,
                        slack_token: slackToken,
                    },
                };
            }
        } else if (contactType === 'Webhook') {
            let webhookValue = container.find('#webhook-id').val();
            let headers = {};
            // Update to use the new header format
            container.find('.headers-container').each(function () {
                const keyInput = $(this).find('#header-key');
                const valueInput = $(this).find('#header-value');
                // Only include headers if both key and value inputs exist and are not empty
                if (keyInput.length && valueInput.length && keyInput.val() && valueInput.val()) {
                    headers[keyInput.val()] = valueInput.val();
                }
            });
            
            if (webhookValue) {
                contactData = {
                    type: 'webhook',
                    settings: {
                        webhook: webhookValue,
                        headers: headers
                    },
                };
            }
        }
        testContactPointHandler(contactData);
    }
}

function testContactPointHandler(testContactPointData) {
    $.ajax({
        method: 'POST',
        url: '/api/alerts/testContactPoint',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(testContactPointData),
        crossDomain: true,
    })
        .then(function (res) {
            if (res.message) {
                showToast(res.message, 'success');
            }
        })
        .fail(function (jqXHR) {
            let response = jqXHR.responseJSON;
            if (response && response.error) {
                showToast(response.error, 'error');
            }
        });
}