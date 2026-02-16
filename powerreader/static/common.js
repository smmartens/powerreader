const REFRESH_MS = 30000;

function escapeHtml(s) {
    const el = document.createElement("div");
    el.textContent = s;
    return el.innerHTML;
}

function toggleChartVisibility(canvas, noData, chart) {
    canvas.style.display = "none";
    noData.style.display = "block";
    if (chart) { chart.destroy(); }
    return null;
}

function showChart(canvas, noData) {
    canvas.style.display = "block";
    noData.style.display = "none";
}

function initVersionFooter() {
    fetch("/api/version").then(r => r.json()).then(d => {
        document.getElementById("footer-version").textContent = "Powerreader v" + d.version;
    }).catch(() => {});
}
