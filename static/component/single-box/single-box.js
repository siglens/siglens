(function ($) {
  $.fn.singleBox = function (options) {
    var defaults = {
      fillIn: true,
      spanName: "",
      defaultValue: "",
      dataList: [],
      clicked: function () {},
      dataUpdate: false,
      clickedHead: function () {},
    };
    let setting = $.extend(defaults, options || {});
    let curCLick = setting.spanName;
    let dataList = setting.dataList;
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
    function updateDropdown() {
        $(`#${name}-listing`).empty(); // Clear existing items
        dataList.forEach((value, index) => {
          let valId = value.replace(" ", "").toLowerCase();
          $(`#${name}-listing`).append(`
          <div class="single-dropdown-item single-item-${name}" id="single-dropdown-${name}-${valId}" data-index="${index}">${value}</div>
        `);
        });
    }
    $(`#${name}-span-name`).text(
      setting.defaultValue != "" ? setting.defaultValue : setting.spanName
    );
    if (setting.dataList.length > 0) {
      setting.dataList.forEach((value, index) => {
        let valId = value.replace(" ", "").toLowerCase();
        $(`#${name}-listing`).append(
          `<div class="single-dropdown-item single-item-${name}" id="single-dropdown-${name}-${valId}" data-index="${index}">${value}</div>`
        );
      });
    }
    if (setting.defaultValue != "" && !setting.dataUpdate) {
      $(
        `#single-dropdown-${name}-${setting.defaultValue
          .replace(" ", "")
          .toLowerCase()}`
      ).addClass("active");
    }
    let dropdownVisible = false;

    $(`#${name}-listing`).on("click", ".single-dropdown-item", function () {
      $(`.single-item-${name}`).removeClass("active");
      if (!setting.dataUpdate)  $(this).addClass("active");
      curCLick = $(this).text();
      if (setting.fillIn) $(`#${name}-span-name`).text(curCLick);
      $(`#${name}-options`).hide();
      dropdownVisible = false;
      setting.clicked(curCLick);
    });

    $(`#${name}-btn`).click(async function () {
      dropdownVisible = !dropdownVisible;
      $(`#${name}-options`).toggle(dropdownVisible);
      if (dropdownVisible && setting.dataUpdate) {
        if (typeof setting.clickedHead === "function") {
          // If clickedHead is a function, use its result as dataList
          const possiblePromise = setting.clickedHead();

          if (possiblePromise instanceof Promise) {
            // If it's a promise, wait for it to resolve
            dataList = await possiblePromise;
          } else {
            dataList = possiblePromise || setting.dataList;
          }
        }
        updateDropdown();
      }
    });
    // Click event on document to close dropdown when clicking outside
    $(document).on("click", function (event) {
      if (
        !$(event.target).closest(`#${name}-btn`).length &&
        !$(event.target).closest(`#${name}-options`).length
      ) {
        $(`#${name}-options`).hide();
        dropdownVisible = false;
      }
    });
    return this;
  };
})(jQuery);

