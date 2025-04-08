$(document).ready(function () {
    const footerComponent = `
        <span id="year"></span> &copy; SigLens
    `;

    $('#app-footer').prepend(footerComponent);
    $('#cstats-app-footer').prepend(footerComponent);
    $('#year').text(new Date().getFullYear());
});