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

let contactGridDiv = null;
let contactRowData  = [];
var contactData = {};
var allContactsArray;
let contactEditFlag = 0;

/* Contact Form Component - This component is used on both the "contacts.html" and "alert.html" pages. */
const contactFormHTML = `
<form id="contact-form">
<div class="d-flex btn-container">
    <button class="btn" id="cancel-contact-btn" type="button">Cancel</button>
    <button class="btn" id="save-contact-btn" type="submit">Save</button>
</div>
<div class="add-contact-form">
    <div>
        <label for="contact-name">Contact point name</label>
        <input type="text" class="form-control" placeholder="Enter a contact point name" id="contact-name" required >
    </div>
    <div id="main-container">
        <div class="contact-container">
            <div>
                <label for="type">Type</label>
                <div class="dropdown">
                    <button class="btn dropdown-toggle" type="button" id="contact-types"
                        data-toggle="dropdown" aria-haspopup="true" aria-expanded="false"
                        data-bs-toggle="dropdown">
                        <span>Slack</span>
                        <img class="dropdown-arrow orange" src="assets/arrow-btn.svg">
                        <img class="dropdown-arrow blue" src="assets/up-arrow-btn-light-theme.svg">
                    </button>
                    <div class="dropdown-menu box-shadow contact-options">
                        <li id="option-0" class="contact-option active">Slack</li>
                        <li id="option-1" class="contact-option">Webhook</li>
                    </div>
                </div>
            </div>
            <div class="slack-container">
                <div>
                <div style="position: relative;">
                    <label for="slack-channel-id">Channel ID</label>
                    <input type="text" class="form-control" id="slack-channel-id" style="position: relative;" required >
                    <i class="fa fa-info-circle position-absolute info-icon" rel="tooltip" style="display: block; top: 29px;"
            title="Specify channel, private group, or IM channel (can be an encoded ID or a name)."
            id="info-slack-channel-id"></i>
            </div>
                </div>
                <div style="position: relative;">
                <label for="slack-token">Slack API Token</label>
                <input type="text" class="form-control" id="slack-token" style="position: relative;" required>
                    <i class="fa fa-info-circle position-absolute info-icon" rel="tooltip" style="display: block; top: 29px;"
                    title="Provide a Slack bot API token (starts with “xoxb”)."
                    id="info-slack-token"></i>
                </div>
            </div>
            <div class="webhook-container">
                <label for="webhook">Webhook URL</label>
                <input type="text" class="form-control" id="webhook-id">
            </div>
        </div>
    </div>
    <button class="add-new-contact-type btn" type="button">
        <span>
            <img src="./assets/add-icon.svg" class="add-icon">Add new contact type
        </span>
    </button>
</div>
</form>
`;

$(document).ready(function () {

    $('.theme-btn').on('click', themePickerHandler);
    $('#new-contact-point').on('click',initializeContactForm)
    $('#contact-form-container').css('display', 'none');
    getAllContactPoints();
    if(window.location.href.includes("alert.html")){
        initializeContactForm();
    }

    const tooltipIds = ["info-slack-channel-id", "info-slack-token"];

    tooltipIds.forEach(id => {
        $(`#${id}`).tooltip({
            delay: { show: 0, hide: 300 },
            trigger: "click"
        }).on("click", function () {
            $(`#${id}`).tooltip("show");
        });
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest(".tooltip-inner").length === 0) {
            tooltipIds.forEach(id => $(`#${id}`).tooltip("hide"));
        }
    });
});

$(document).on('click', '.contact-option', setContactTypes);

function initializeContactForm(contactId) {
    $("#new-contact-point").css("display", "none");
    $("#alert-grid-container").css("display", "none");
    $("#contact-form-container").css("display", "block");
    const formContainer = $("#contact-form-container");

    if (formContainer) {
        formContainer.append(contactFormHTML);
        $(".slack-container").css("display", "block");
        $(".webhook-container").css("display", "none");
        if (contactEditFlag) {
            showContactFormForEdit(contactId);
        }
    }

    const contactForm = $("#contact-form");
    contactForm.on("submit", (e) => submitAddContactPointForm(e));

    //cancel form
    $("#cancel-contact-btn").on("click", function () {
        resetContactForm();
        if (window.location.href.includes("alert.html")) {
            $(".popupOverlay, .popupContent").removeClass("active");
            $("#main-container .contact-container:gt(0)").remove();
        } else {
            window.location.href = "../contacts.html";
        }
    });

    //add new contact type container
    $(".add-new-contact-type").on("click", function () {
        let newContactContainer = $(".contact-container").first().clone();
        newContactContainer.find(".form-control").val("");
        newContactContainer.prepend(
            '<button class="btn-simple del-contact-type" type="button"></button>',
        );
        newContactContainer.appendTo("#main-container");
        const newChannelIdInfoId = "info-slack-channel-id-" + Date.now();
        const newTokenInfoId = "info-slack-token-" + Date.now();

        newContactContainer.find('.fa-info-circle').eq(0).attr('id', newChannelIdInfoId);
        newContactContainer.find('.fa-info-circle').eq(1).attr('id', newTokenInfoId);

        // Initialize tooltips for the newly added container
        [newChannelIdInfoId, newTokenInfoId].forEach(id => {
            $(`#${id}`).tooltip({
                delay: { show: 0, hide: 300 },
                trigger: "hover" // Change "click" to "hover"
            });
        });
    });

    //remove contact type container
    $("#main-container").on("click", ".del-contact-type", function () {
        $(this).closest(".contact-container").remove();
    });
}

function setContactTypes() {
    const selectedOption = $(this).html();
    const container = $(this).closest('.contact-container');
    container.find('.contact-option').removeClass('active');
    container.find('#contact-types span').html(selectedOption);
    $(this).addClass('active');
    container.find('.slack-container input, .webhook-container input').removeAttr('required').val();
    container.find('.slack-container, .webhook-container').css('display', 'none');
    if (selectedOption === 'Slack') {
        container.find('.slack-container input').attr('required', 'true');
        container.find('.slack-container').css('display', 'block');
    } else if (selectedOption === 'Webhook') {
        container.find('.webhook-container').css('display', 'block');
        container.find('.webhook-container input').attr('required', 'true');
    } 
}

function resetContactForm(){
    $('#contact-form input').val('');
    $('#contact-types span').text('Slack');
    $('.slack-container').css('display', 'block');
    $('.webhook-container').css('display', 'none');
    $('.contact-option').removeClass('active');
    $('.contact-options #option-0').addClass('active');
    contactData = {};
}

function setContactForm() {
    contactData.contact_name = $('#contact-name').val();
    contactData.slack = [];
    contactData.webhook = [];
    
    $('.contact-container').each(function() {
      let contactType = $(this).find('#contact-types span').text();
      
        if (contactType === 'Slack') {
            let slackValue = $(this).find('#slack-channel-id').val();
            let slackToken = $(this).find('#slack-token').val();
            if (slackValue && slackToken) {
                let slackContact  = {
                    channel_id: slackValue,
                    slack_token: slackToken
                    };
                    contactData.slack.push(slackContact);

          
        }
      } else if (contactType === 'Webhook') {
        let webhookValue = $(this).find('#webhook-id').val();
        if (webhookValue) {
            let webhookContact  = {
                webhook: webhookValue,
                };
            contactData.webhook.push(webhookContact);
        }
    }
    });
    contactData.pager_duty = "";
}

function submitAddContactPointForm(e){
    e.preventDefault(); 
    setContactForm();
    contactEditFlag ? updateContactPoint(contactData) : createContactPoint(contactData);
}

function createContactPoint(){
    $.ajax({
        method: "post",
        url: "api/alerts/createContact",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify(contactData),
        dataType: 'json',
        crossDomain: true,
    }).then(res=>{
        if(window.location.href.includes("alert.html")){
            $(".popupOverlay, .popupContent").removeClass("active");
            getAllContactPoints(contactData.contact_name);
        } else {
            window.location.href = "../contacts.html";
        }
        resetContactForm();
        showToast(res.message);
    }).catch(err=>{
        if (window.location.href.includes("alert.html")) {
            $(".popupOverlay, .popupContent").removeClass("active");
        }
        showToast(err.responseJSON.error);
    })
}

function updateContactPoint(){
    $.ajax({
        method: "post",
        url: "api/alerts/updateContact",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify(contactData),
        dataType: 'json',
        crossDomain: true,
    }).then(res=>{
        resetContactForm();
        window.location.href='../contacts.html';
        showToast(res.message);
    }).catch(err=>{
        showToast(err.responseJSON.error);
    })
}

function getAllContactPoints(contactName){
    $.ajax({
        method: "get",
        url: "api/alerts/allContacts",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        allContactsArray = res.contacts;
        if(window.location.href.includes("alert.html")){
            const contact = allContactsArray.find(contact => contact.contact_name === contactName);
            $('#contact-points-dropdown span').html(contact.contact_name);
            $('#contact-points-dropdown span').attr('id', contact.contact_id);
        }else
            displayAllContacts(res.contacts);
    })
}

class btnRenderer {
	init(params) {
		this.eGui = document.createElement('span');
		this.eGui.innerHTML = `<div id="alert-grid-btn">
				<button class='btn' id="editbutton"></button>
                <button class="btn-simple" id="delbutton"></button>
				</div>`;
		this.eButton = this.eGui.querySelector('.btn');
		this.dButton = this.eGui.querySelector('.btn-simple');

		this.eButton.addEventListener('click',function(){
            contactEditFlag = 1;
            initializeContactForm(params.data.contactId);
        });   
		this.dButton.addEventListener('click',()=>getAllAlertsWithSameContactPoint(params.data));
	}

	getGui() {
		return this.eGui;
	}
	refresh(params) {
		return false;
	}
}

function deleteContactPrompt(data) {
    $('#contact-name-placeholder').html('<strong>' + data.contactName + '</strong>');
    $('.popupOverlay, .popupContent').addClass('active');
    $('#cancel-btn, .popupOverlay').click(function () {
        $('.popupOverlay, .popupContent').removeClass('active');
    });
    $('#delete-btn').off('click');
    $('#delete-btn').on('click',function (){
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
        }).then(function (res) {
            let deletedRowID = data.rowId;
            contactGridOptions.api.applyTransaction({
                remove: [{ rowId: deletedRowID }],
            });

            showToast(res.message);
            $('.popupOverlay, .popupContent').removeClass('active');
        });
    });
}

function showDeleteContactDialog(data,matchingAlertNames){
    $('#contact-name-placeholder-delete-dialog').html('<strong>' + data.contactName + '</strong>');
    $('.popupOverlay, .delete-dialog').addClass('active');
    let el = $('#alert-listing');
    el.html(``);
    const maxHeight = 100;
    matchingAlertNames.forEach(function (alertName) {
        el.append(`<div class="alert-dropdown-item">${alertName}</div>`);
    });

    // Apply styling to make the dropdown scrollable
el.css({
    'max-height': `${maxHeight}px`,
    'overflow-y': 'auto'
});
    $("body").css("cursor","default");
    $('#cancel-btn, .popupOverlay').click(function () {
        $('.popupOverlay, .delete-dialog').removeClass('active');
    });
}

function getAllAlertsWithSameContactPoint(data){
    const contactId= data.contactId;
    const matchingAlertNames = [];
    $.ajax({
        method: "get",
        url: "api/allalerts",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        if(res.alerts){  
            for (const alert of res.alerts) {
            if (alert.contact_id === contactId) {
                matchingAlertNames.push(alert.alert_name);
        }}}
          if(matchingAlertNames.length > 0){
            showDeleteContactDialog(data,matchingAlertNames);
          }else{
            deleteContactPrompt(data);
          }
    })
}


let contactColumnDefs = [
    {
        field: "rowId",
		hide: true
    },
    {
        field: "contactId",
		hide: true
    },
    {
        headerName: "Contact point name",
        field: "contactName",
        sortable: true,
        width:100,
    },
    {
        headerName: "Type",
        field: "type",
        width:100,
    },
    {
        headerName: "Actions",
        cellRenderer: btnRenderer,
        width:50,
    },
];

const contactGridOptions = {
    columnDefs: contactColumnDefs,
	rowData: contactRowData,
	animateRows: true,
	rowHeight: 44,
    headerHeight:32,
	defaultColDef: {
		icons: {
			sortAscending: '<i class="fa fa-sort-alpha-up"/>',
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

function displayAllContacts(res){
    if (contactGridDiv === null) {
        contactGridDiv = document.querySelector('.all-contacts-grid');
        new agGrid.Grid(contactGridDiv, contactGridOptions);
    }
    contactGridOptions.api.setColumnDefs(contactColumnDefs);
    let newRow = new Map()
    $.each(res, function (key, value) {
        let contactType = 
        Object.entries(value)
        .filter(([k,v]) => v!=null && v.length !== 0 && k !== 'contact_name' && k !== 'contact_id' && k !== 'org_id')
        .map(([k, v]) => {
            return v.length >1 ? `${capitalize(k)} (${v.length})` : capitalize(k);
        } );
        let type = contactType.join(', ')

        newRow.set("rowId", key);
        newRow.set("contactId", value.contact_id);
        newRow.set("contactName", value.contact_name);
        newRow.set("type", type);
        contactRowData = _.concat(contactRowData, Object.fromEntries(newRow));
    })
    contactGridOptions.api.setRowData(contactRowData);
    contactGridOptions.api.sizeColumnsToFit();
}

function showToast(msg) {
    let toast =
        `<div class="div-toast" id="save-db-modal"> 
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast)
    setTimeout(removeToast, 2000);
}

function removeToast() {
    $('.div-toast').remove();
}

//Edit Contact Point 
function showContactFormForEdit(contactId) {
    let data = allContactsArray.find(function(obj) {return obj.contact_id === contactId});
    $('#contact-name').val(data.contact_name);
    
    let isFirst = true;
    Object.keys(data).forEach(function(key) {
      if (key === 'contact_name' || key === 'contact_id') {
        return;
      }
    
      let value = data[key];
      if (value != null && value.length > 0) {
       value.forEach(function(value){ let contactContainer;
        if (isFirst) {
            contactContainer = $('.contact-container');
            isFirst = false;
        } else {
            contactContainer = $('.contact-container').first().clone();
            contactContainer.prepend('<button class="btn-simple del-contact-type" type="button"></button>');
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

          }
        if (key != 'slack'&& key != 'webhook'){
            contactContainer.find(`.${key}-container .form-control`).val(value);
        }
        contactContainer.appendTo('#main-container');
      })}
    });
    if(contactEditFlag){contactData.contact_id=data.contact_id;}
}