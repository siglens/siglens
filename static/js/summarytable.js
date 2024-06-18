document.addEventListener("DOMContentLoaded", () => {
    const metricsPerPage = 28;
    let currentPage = 1;
    let metrics = [];
    let sortedMetrics = [];

    const fetchMetrics = async () => {
        const response = await fetch('http://playground.siglens.com:5122/metrics-explorer/api/v1/metric_names', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                start: "now-lh",
                end: "now"
            })
        });
        const data = await response.json();
        metrics = data.metricNames;
        sortedMetrics = [...metrics].sort((a, b) => a.localeCompare(b));
        renderTable(sortedMetrics); // Initial render with sorted metrics
        renderPagination();
    };

    const renderTable = (metricsToRender) => {
        const indexOfLastMetric = currentPage * metricsPerPage;
        const indexOfFirstMetric = indexOfLastMetric - metricsPerPage;
        const currentMetrics = metricsToRender.slice(indexOfFirstMetric, indexOfLastMetric);

        const metricsTable = document.getElementById("metricsTable");
        metricsTable.innerHTML = "";
        currentMetrics.forEach((metric, index) => {
            const row = document.createElement("tr");
            row.className = "met";
            row.innerHTML = `
                <td>${metric}</td>
                <td style="display: flex; flex-direction: row; justify-content: space-between;">
                    -
                </td>
            `;
            metricsTable.appendChild(row);
        });
    };

    const renderPagination = () => {
        const totalPages = Math.ceil(sortedMetrics.length / metricsPerPage);
        const pagination = document.getElementById("pagination");
        pagination.innerHTML = "";

        const createButton = (text, disabled, pageNumber) => {
            const button = document.createElement("button");
            button.textContent = text;
            button.disabled = disabled;
            button.addEventListener("click", () => {
                currentPage = pageNumber;
                renderTable(sortedMetrics);
                renderPagination();
            });
            return button;
        };

        pagination.appendChild(createButton("←", currentPage === 1, currentPage - 1));

        for (let i = 1; i <= totalPages; i++) {
            const button = createButton(i, false, i);
            if (i === currentPage) {
                button.classList.add("active");
            }
            pagination.appendChild(button);
        }

        pagination.appendChild(createButton("→", currentPage === totalPages, currentPage + 1));
    };

    const filterMetrics = (searchText) => {
        const filteredMetrics = metrics.filter(metric =>
            metric.toLowerCase().includes(searchText.toLowerCase())
        );
        renderTable(filteredMetrics);
        currentPage = 1; // Reset pagination to first page
        renderPagination();
    };

    const metricInput = document.getElementById("metric");
    metricInput.addEventListener("input", (event) => {
        const searchText = event.target.value.trim();
        filterMetrics(searchText);
    });

    fetchMetrics();
});
