

$(document).ready(function () {
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
        
    }
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


