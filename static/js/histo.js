$(document).ready(function () {
    $("#toggle-btn").click(function (event) {
        event.stopPropagation();
        $("#histogram-container").slideToggle();
        renderHistogram();
    });

    $("#histogram-container").click(function (event) {
        event.stopPropagation();
    });

    // Optional: Close when clicking outside the histogram
    $(document).click(function () {
        $("#histogram-container").slideUp();
    });

    function renderHistogram() {
        const logs = [
            { timestamp: "2025-02-10", count: 1200, error: "Server Error" },
            { timestamp: "2025-02-11", count: 1800, error: "Database Error" },
            { timestamp: "2025-02-12", count: 500, error: "Memory Leak" },
            { timestamp: "2025-02-13", count: 900, error: "Network Timeout" },
            { timestamp: "2025-02-14", count: 600, error: "File Not Found" },
            { timestamp: "2025-02-15", count: 1500, error: "Unauthorized Access" },
            { timestamp: "2025-02-16", count: 1700, error: "High CPU Usage" },
            { timestamp: "2025-02-17", count: 2000, error: "System Crash" },
            { timestamp: "2024-02-10", count: 1200, error: "Server Error" },
            { timestamp: "2024-02-11", count: 1800, error: "Database Error" },
            { timestamp: "2024-02-12", count: 500, error: "Memory Leak" },
            { timestamp: "2024-02-13", count: 900, error: "Network Timeout" },
            { timestamp: "2024-02-14", count: 600, error: "File Not Found" },
            { timestamp: "2024-02-15", count: 1500, error: "Unauthorized Access" },
            { timestamp: "2024-02-16", count: 1700, error: "High CPU Usage" },
            { timestamp: "2024-02-17", count: 2000, error: "System Crash" }

        ];

        $("#histogram").empty();
        $("#x-axis").empty();
        $("#y-axis").empty();

        let yValues = [2000, 1500, 1000, 500, 0];
        yValues.forEach(value => {
            $("#y-axis").append(`<span>${value}</span>`);
        });

        let maxCount = 2000;

        logs.forEach(log => {
            let count = log.count;
            let barHeight = (count / maxCount) * 100;

            let bar = `
                <div class="bar" style="height: ${barHeight}%;"
                    data-timestamp="${log.timestamp}"
                    data-count="${count}"
                    data-error="${log.error}">
                </div>`;
            $("#histogram").append(bar);
            $("#x-axis").append(`<span>${log.timestamp}</span>`);
        });

        // Tooltip handling
        $(".bar").hover(function () {
            let timestamp = $(this).data("timestamp");
            let count = $(this).data("count");
            let error = $(this).data("error");

            $(this).attr("title", `Date: ${timestamp}\nCount: ${count}\nError: ${error}`);
        });
    }
});
