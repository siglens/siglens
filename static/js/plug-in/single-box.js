(function ($) {
  $.fn.singleBox = function (options) {
    var defaults = {
      spanName: "",
      dataList: []
    };
    let setting = $.extend(defaults, options || {});
      this.html(``);
      let name = setting.spanName.toLowerCase();
      this
        .append(`<button class="btn dropdown-toggle" type="button" id="${name}-btn" data-toggle="dropdown" aria-haspopup="true"
                                aria-expanded="false" data-bs-toggle="dropdown" title="Index Name to search on">
                                <span id="${name}-span-name"></span>
                                <img class="dropdown-arrow orange" src="assets/arrow-btn.svg">
                                <img class="dropdown-arrow blue" src="assets/up-arrow-btn-light-theme.svg">
                            </button>
                            <div class="dropdown-menu box-shadow dropdown-plugin" aria-labelledby="index-btn" id="${name}-options">
                                <div id="${name}-listing"></div>
                            </div>`);
      $(`#${name}-span-name`).text(setting.spanName);
      if (setting.dataList.length > 0) {
        setting.dataList.forEach((value, index) => {
          $(`#${name}-listing`)
            .append(`<div class="index-dropdown-item" data-index="${index}">${value}</div>`);
        });
      }
      $(`#${name}-listing`).on("click", ".index-dropdown-item", function () {
        let curCLick = $(this).text();
        $(`#${name}-span-name`).text(curCLick);
      });
    return this;
  };
})(jQuery);

