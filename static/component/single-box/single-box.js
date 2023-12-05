(function ($) {
  $.fn.singleBox = function (options) {
    var defaults = {
      fillIn: true,
      spanName: "",
      defaultValue: "",
      dataList: [],
      clicked:function(){}
    };
    let setting = $.extend(defaults, options || {});
    let curCLick = setting.spanName;
      this.html(``);
      let name = setting.spanName.toLowerCase().replace(" ", "");
      this
        .append(`<button class="btn dropdown-toggle ${name}-btn" type="button" id="${name}-btn" data-toggle="dropdown" aria-haspopup="true"
                                aria-expanded="false" data-bs-toggle="dropdown" title="Index Name to search on">
                                <span class = "span-name-index" id="${name}-span-name"></span>
                                <div class="dropdown-arrow-show"></div>
                            </button>
                            <div class="dropdown-menu box-shadow dropdown-plugin" aria-labelledby="index-btn" id="${name}-options">
                                <div id="${name}-listing"></div>
                            </div>`);
      $(`#${name}-span-name`).text(setting.defaultValue != "" ? setting.defaultValue : setting.spanName);
        if (setting.dataList.length > 0) {
          setting.dataList.forEach((value, index) => {
            let valId = value.replace(" ", "").toLowerCase();
            $(`#${name}-listing`).append(
              `<div class="single-dropdown-item single-item-${name}" id="single-dropdown-${name}-${valId}" data-index="${index}">${value}</div>`
            );
          });
        }
      if (setting.defaultValue!="") {
        $(
          `#single-dropdown-${name}-${setting.defaultValue
            .replace(" ", "")
            .toLowerCase()}`
        ).addClass("active");
      }

      $(`#${name}-listing`).on("click", ".single-dropdown-item", function () {
        $(`.single-item-${name}`).removeClass("active");
        $(this).addClass("active");
        curCLick = $(this).text();
        if (setting.fillIn) $(`#${name}-span-name`).text(curCLick);
      });
      const data = {
        index: curCLick
      };
      //callback function
      $(`#${name}-listing`).on("click", ".single-dropdown-item", data, setting.clicked);
      return this;
  };
})(jQuery);

