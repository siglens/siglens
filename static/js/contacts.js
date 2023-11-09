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

'use strict';

let contactGridDiv = null;
let contactRowData  = [];
var contactData = {};
var allContactsArray;
let contactEditFlag = 0;

/* Contact Form Component - This component is used on both the "contacts.html" and "alert.html" pages. */
const contactFormHTML = `
<form id="contact-form">
<div class="add-contact-form">
    <div>
        <label for="contact-name">Name</label>
        <input type="text" class="form-control" placeholder="Name" id="contact-name" required >
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
                <label for="slack">Channel ID</label>
                <input type="text" class="form-control" id="slack-channel-id">
                <label for="slack">Slack Token</label>
                <input type="text" class="form-control" id="slack-token">

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
    <div>
        <button class="btn" id="cancel-contact-btn" type="button">Cancel</button>
        <button class="btn" id="save-contact-btn" type="submit">Save</button>
    </div>
</div>
</form>
`;

$(document).ready(function () {
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
        
    }
    $('.theme-btn').on('click', themePickerHandler);
    displayNavbar();

    $('#new-contact-point').on('click',initializeContactForm)
    $('#contact-form-container').css('display', 'none');
    getAllContactPoints();

    if(window.location.href.includes("alert.html")){
        initializeContactForm();
    }
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
        $(this).blur();
    });

    //remove contact type container
    $("#main-container").on("click", ".del-contact-type", function () {
        $(this).closest(".contact-container").remove();
    });
}

function setContactTypes() {
    $('#main-container input').val('');
    const selectedOption = $(this).html();
    const container = $(this).closest('.contact-container');
    container.find('.contact-option').removeClass('active');
    container.find('#contact-types span').html(selectedOption);
    $(this).addClass('active');
    container.find('.slack-container, .webhook-container').css('display', 'none');
    if (selectedOption === 'Slack') {
        container.find('.slack-container').css('display', 'block');
    } else if (selectedOption === 'Webhook') {
        container.find('.webhook-container').css('display', 'block');
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
            contactData.webhook.push(webhookValue);
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

function showDeleteContactDialog(){
    $('.popupOverlay, .delete-dialog').addClass('active');
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
            showDeleteContactDialog();
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
	defaultColDef: {
		icons: {
			sortAscending: '<i class="fa fa-sort-alpha-up"/>',
			sortDescending: '<i class="fa fa-sort-alpha-down"/>',
		},
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
        .filter(([k,v]) => v!=null && v.length !== 0 && k !== 'contact_name' && k !== 'contact_id')
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
          }
        if (key != 'slack'){
            contactContainer.find(`.${key}-container .form-control`).val(value);
        }
        contactContainer.appendTo('#main-container');
      })}
    });
    if(contactEditFlag){contactData.contact_id=data.contact_id;}
}